package core

import (
	"bufio"
	"io"
	"regexp"
	"strings"

	. "github.com/zyguan/sqlz/stmtflow"
)

var re = regexp.MustCompile(`^/\*\s*(\w+)(:[\w,]+)?\s*\*/\s+(.*);.*$`)

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
