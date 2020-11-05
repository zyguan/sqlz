package format

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"regexp"
	"strings"

	. "github.com/zyguan/sqlz/stmtflow"
)

var re = regexp.MustCompile(`^/\*\s*(\w+)(:[\w,]+)?\s*\*/\s+(.*);.*$`)

type TextDumpOptions struct {
	Verbose bool
	WithLat bool
}

func TextEventDumper(w io.Writer, opts TextDumpOptions) func(Event) {
	return func(e Event) {
		switch e.Kind {
		case EventInvoke:
			fmt.Fprintln(w, e.Invoke().Stmt.SQL)
		case EventReturn:
			ret := e.Return()
			if ret.Err == nil {
				if opts.Verbose && !ret.Res.IsExecResult() {
					buf, fst := new(bytes.Buffer), true
					ret.Res.PrettyPrint(buf)
					for {
						line, err := buf.ReadString('\n')
						if err != nil {
							break
						}
						if fst {
							fmt.Fprint(w, "-- ", e.Session, " >> ", line)
							fst = false
						} else {
							fmt.Fprint(w, "-- ", e.Session, "    ", line)
						}
					}
				} else {
					fmt.Fprintf(w, "-- %s >> %s\n", e.Session, ret.Res.String())
				}
				if opts.WithLat {
					fmt.Fprintf(w, "-- %s    %s ~ %s (cost %s)\n", e.Session,
						ret.T[0].Format("15:04:05.000"), ret.T[1].Format("15:04:05.000"), ret.T[1].Sub(ret.T[0]))
				}
			} else {
				fmt.Fprintf(w, "-- %s >> %s\n", e.Session, ret.Err.Error())
			}
		case EventBlock:
			fmt.Fprintf(w, "-- %s >> blocked\n", e.Session)
		case EventResume:
			fmt.Fprintf(w, "-- %s >> resumed\n", e.Session)
		}
	}
}

func ParseSQL(r io.Reader) []Stmt {
	lst := make([]Stmt, 0, 16)
	in := bufio.NewScanner(r)
	in.Split(bufio.ScanLines)
	for in.Scan() {
		line := in.Text()
		sess, stmt, marks := split(line)
		if len(sess) == 0 {
			continue
		}
		var flags uint
		if isQuery(stmt) {
			flags |= S_QUERY
		}
		for _, m := range marks {
			switch strings.ToLower(m) {
			case "wait":
				flags |= S_WAIT
			case "unordered":
				flags |= S_UNORDERED
			}
		}
		lst = append(lst, Stmt{
			Sess:  sess,
			SQL:   line,
			Flags: flags,
		})
	}
	return lst
}

func split(line string) (string, string, []string) {
	ss, marks := re.FindStringSubmatch(line), []string{}
	if len(ss) != 4 {
		return "", "", marks
	}
	if len(ss[2]) > 0 {
		for _, s := range strings.Split(ss[2][1:], ",") {
			if len(s) > 0 {
				marks = append(marks, s)
			}
		}
	}
	return ss[1], ss[3], marks
}

func isQuery(sql string) bool {
	// a naive impl
	sql = strings.ToLower(strings.TrimLeft(strings.TrimSpace(sql), "("))
	for _, w := range []string{"select ", "show ", "admin show ", "explain ", "desc ", "describe "} {
		if strings.HasPrefix(sql, w) {
			return true
		}
	}
	return false
}
