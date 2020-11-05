package test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/google/go-jsonnet/ast"
	"github.com/pingcap/errors"
	"github.com/zyguan/sqlz/tools/stmtflow/format"

	. "github.com/google/go-jsonnet"
	. "github.com/zyguan/sqlz/stmtflow"
)

var (
	ErrNotAssertable = errors.New("does not know how to assert")
)

type Meta struct {
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels"`
}

type Test struct {
	Meta     `json:",inline"`
	Flow     []Stmt  `json:"flow"`
	Expect   []Event `json:"expect"`
	Negative bool    `json:"negative"`

	Check func(history History) error
}

func (t *Test) Assert(actual []Event) error {
	err := t.assert(actual)
	if t.Negative && err == nil {
		return errors.New("negative test expects to be failed")
	} else if !t.Negative && err != nil {
		return err
	}
	return nil
}

func (t *Test) assert(actual []Event) error {
	if len(t.Expect) == 0 && t.Check == nil {
		return ErrNotAssertable
	}
	if len(t.Expect) > 0 && len(t.Expect) != len(actual) {
		return fmt.Errorf("expect %d events, got %d", len(t.Expect), len(actual))
	}
	for i := range t.Expect {
		if ok, msg := t.Expect[i].EqualTo(actual[i]); !ok {
			return fmt.Errorf("#%d %s", i, msg)
		}
	}
	if t.Check != nil {
		return t.Check(actual)
	}
	return nil
}

type Suite struct {
	Tests       []Test   `json:"tests"`
	ExpectTypes []string `json:"expects"`
}

type customAssertion struct {
	path string
	idx  int
}

func (a *customAssertion) Assert(history History) error {
	vm := initVM(MakeVM())
	h, err := json.Marshal(history)
	if err != nil {
		return errors.Trace(err)
	}
	src := fmt.Sprintf(`local s = import "%s"; s[%d].expect(%s)`, a.path, a.idx, string(h))

	js, err := vm.EvaluateSnippet(":assert:", src)
	if err != nil {
		return errors.Trace(err)
	}
	ok := false
	if err := json.Unmarshal([]byte(js), &ok); err != nil {
		return errors.Errorf("assert failed: %s, unexpected outcome: %s", err, js)
	}
	if !ok {
		return errors.New("assert failed")
	}
	return nil
}

func Load(path string) (*Suite, error) {
	vm := initVM(MakeVM())
	src := fmt.Sprintf(`local tests = import "%s";
{
    tests: tests,
    expects: std.map(function(test) std.type(test.expect), tests),
}`, path)

	js, err := vm.EvaluateSnippet(":load:", src)
	if err != nil {
		return nil, errors.Annotate(err, "load tests")
	}
	var s Suite
	if err = json.Unmarshal([]byte(js), &s); err != nil {
		return nil, errors.Annotate(err, "parse tests")
	}
	for i := range s.Tests {
		if s.ExpectTypes[i] == "function" {
			a := customAssertion{path, i}
			s.Tests[i].Check = a.Assert
		}
	}
	return &s, nil
}

var nativeFuncs = map[string]*NativeFunction{
	"parseSQL": {
		Name:   "parseSQL",
		Params: ast.Identifiers{"sql"},
		Func:   nativeParseSQL,
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
	stmts := format.ParseSQL(buf)
	buf.Reset()
	if err = json.NewEncoder(buf).Encode(stmts); err != nil {
		return
	}
	if err = json.NewDecoder(buf).Decode(&ret); err != nil {
		return
	}
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
	return ei.fi.Import(from, path)
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
