package core

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/go-jsonnet/ast"
	"github.com/pingcap/errors"

	. "github.com/google/go-jsonnet"
	. "github.com/zyguan/sqlz/stmtflow"
)

const srcExpect = `# eval expect function
local tests = import "__PATH__";
local test = tests["__NAME__"];
test.expect(std.extVar("actual"))`

const srcLoad = `# load & filter tests
local tests = import "__PATH__";
local filter(test) = __FILTER__; # default to true
local patch(name) = {name: name, assertMethod: std.type(super.expect)};
[
	tests[name] + patch(name)
	for name in std.objectFields(tests)
	if filter(tests[name] + patch(name))
]`

const srcLib = `# builtin lib
{
	parseSQL(sql):: std.native("parseSQL")(sql),
	dumpText(history, verbose=true, withLat=false):: std.native("historyToText")(history, verbose, withLat),
	textContains(str, sub):: std.length(std.findSubstr(sub, str)) > 0,
	historyContains(history, sub):: self.textContains(self.dumpText(history), sub),
}`

func Load(path string, filter string) ([]Test, error) {
	if len(filter) == 0 {
		filter = "true"
	}
	vm := initVM(MakeVM())
	src := srcLoad
	src = strings.Replace(src, "__PATH__", path, 1)
	src = strings.Replace(src, "__FILTER__", filter, 1)
	js, err := vm.EvaluateSnippet(":load:", src)
	if err != nil {
		return nil, errors.Annotate(err, "load tests")
	}

	var tests []Test
	if err = json.Unmarshal([]byte(js), &tests); err != nil {
		return nil, errors.Trace(err)
	}
	for i, t := range tests {
		switch t.AssertMethod {
		case "string":
			var a matchText
			if err = json.Unmarshal(t.Expect, &a.expect); err != nil {
				return nil, errors.Annotate(err, "unmarshal "+t.AssertMethod+" `expect` of "+t.Name)
			}
			t.Assertions = append(t.Assertions, &a)
		case "array":
			var a matchHistory
			if err = json.Unmarshal(t.Expect, &a.expect); err != nil {
				return nil, errors.Annotate(err, "unmarshal "+t.AssertMethod+" `expect` of "+t.Name)
			}
			t.Assertions = append(t.Assertions, &a)
		case "function":
			t.Assertions = append(t.Assertions, &customAssertFn{path, t.Name})
		default:
			return nil, errors.New("unexpected assert method: " + t.AssertMethod)
		}
		tests[i] = t
	}

	return tests, nil
}

var nativeFuncs = map[string]*NativeFunction{
	"parseSQL": {
		Name:   "parseSQL",
		Params: ast.Identifiers{"sql"},
		Func:   nativeParseSQL,
	},
	"historyToText": {
		Name:   "historyToText",
		Params: ast.Identifiers{"history", "verbose", "withLat"},
		Func:   nativeHistoryToText,
	},
}

func initVM(vm *VM) *VM {
	vm.Importer(newImporter())
	for _, f := range nativeFuncs {
		vm.NativeFunction(f)
	}
	return vm
}

func nativeParseSQL(args []interface{}) (ret interface{}, err error) {
	defer catchPanic(&err)
	buf := bytes.NewBuffer([]byte(args[0].(string)))
	stmts := ParseSQL(buf)
	buf.Reset()
	if err = json.NewEncoder(buf).Encode(stmts); err != nil {
		return nil, errors.Trace(err)
	}
	if err = json.NewDecoder(buf).Decode(&ret); err != nil {
		return nil, errors.Trace(err)
	}
	return
}

func nativeHistoryToText(args []interface{}) (ret interface{}, err error) {
	defer catchPanic(&err)
	var h History
	buf := new(bytes.Buffer)
	if err = json.NewEncoder(buf).Encode(args[0]); err != nil {
		return nil, errors.Trace(err)
	}
	if err = json.NewDecoder(buf).Decode(&h); err != nil {
		return nil, errors.Trace(err)
	}
	buf.Reset()
	err = h.DumpText(buf, TextDumpOptions{Verbose: args[1].(bool), WithLat: args[2].(bool)})
	if err != nil {
		return nil, errors.Trace(err)
	}
	ret = buf.String()
	return
}

func catchPanic(err *error) {
	if x := recover(); x != nil {
		if e, ok := x.(error); ok {
			*err = e
		} else {
			*err = fmt.Errorf("unexpected panic: %v", x)
		}
	}
}

func newImporter() *enhancedImporter {
	t := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	return &enhancedImporter{
		fi:    &FileImporter{},
		http:  &http.Client{Transport: t},
		cache: map[string]Contents{},
	}
}

type enhancedImporter struct {
	fi    *FileImporter
	http  *http.Client
	cache map[string]Contents
}

func (ei *enhancedImporter) Import(from string, path string) (Contents, string, error) {
	pathURL, err := url.Parse(path)
	if err != nil {
		return Contents{}, "", errors.New("import path `" + path + "` is not valid")
	}
	if pathURL.Scheme == "http" || pathURL.Scheme == "https" {
		foundAt := pathURL.String()
		if c, ok := ei.cache[foundAt]; ok {
			return c, foundAt, nil
		}
		c, err := ei.importViaHttp(foundAt)
		if err != nil {
			return Contents{}, "", errors.Annotate(err, "import via http")
		}
		ei.cache[foundAt] = c
		return c, foundAt, nil
	}
	c, p, err := ei.fi.Import(from, path)
	if err != nil && (path == "stmtflow" || path == "stmtflow.libsonnet") {
		return MakeContents(srcLib), ":builtin:", nil
	}
	return c, p, err
}

func (ei *enhancedImporter) importViaHttp(url string) (Contents, error) {
	resp, err := ei.http.Get(url)
	if err != nil {
		return Contents{}, errors.Trace(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return Contents{}, errors.New("unexpected http status: " + resp.Status)
	}

	bs, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return Contents{}, errors.Annotate(err, "error reading content")
	}
	return MakeContents(string(bs)), nil
}
