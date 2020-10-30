package main

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/zyguan/sqlz/stmtflow"

	_ "github.com/go-sql-driver/mysql"
	. "github.com/zyguan/just"
)

var re = regexp.MustCompile(`^/\*\s*(\w+)(:\w+)?\s*\*/\s+(.*);.*$`)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func readInput(r io.Reader) []stmtflow.Stmt {
	lst := make([]stmtflow.Stmt, 0, 16)
	in := bufio.NewScanner(r)
	in.Split(bufio.ScanLines)
	for in.Scan() {
		line := in.Text()
		sess, ann, stmt := split(line)
		if len(sess) == 0 {
			continue
		}
		var flags uint
		if isQuery(stmt) {
			flags |= stmtflow.S_QUERY
		}
		if strings.ToLower(ann) == ":wait" {
			flags |= stmtflow.S_WAIT
		}
		lst = append(lst, stmtflow.Stmt{
			Sess:  sess,
			SQL:   stmt,
			Flags: flags,
		})
	}
	return lst
}

func split(line string) (string, string, string) {
	ss := re.FindStringSubmatch(line)
	if len(ss) != 4 {
		return "", "", ""
	}
	return ss[1], ss[2], ss[3]
}

func isQuery(sql string) bool {
	sql = strings.ToLower(strings.TrimLeft(strings.TrimSpace(sql), "("))
	for _, w := range []string{"select ", "show ", "admin show ", "explain ", "desc ", "describe "} {
		if strings.HasPrefix(sql, w) {
			return true
		}
	}
	return false
}

func main() {
	defer Catch(func(c Catchable) {
		fmt.Fprintf(os.Stderr, "\x1b[0;31mError: %+v\x1b[0m\n", c.Why())
	})
	var opts struct {
		dsn     string
		input   string
		verbose bool
		withLat bool

		pingTime  time.Duration
		blockTime time.Duration
	}
	flag.StringVar(&opts.dsn, "dsn", "root:@tcp(127.0.0.1:4000)/test", "data source name")
	flag.StringVar(&opts.input, "i", "", "input file (default to stdin)")
	flag.BoolVar(&opts.verbose, "v", false, "verbose (show select results)")
	flag.BoolVar(&opts.withLat, "with-lat", false, "show statement latency")
	flag.DurationVar(&opts.pingTime, "ping-time", 200*time.Millisecond, "max wait time to ping a blocked stmt")
	flag.DurationVar(&opts.blockTime, "block-time", 2*time.Second, "max wait time to run a stmt synchronously")

	flag.Parse()

	db := Try(sql.Open("mysql", opts.dsn)).(*sql.DB)
	defer db.Close()

	f := os.Stdin
	if len(opts.input) > 0 {
		f = Try(os.Open(opts.input)).(*os.File)
		defer f.Close()
	}

	w, err := stmtflow.Eval(context.TODO(), db, readInput(f), stmtflow.EvalOptions{
		PingTime:  opts.pingTime,
		BlockTime: opts.blockTime,
		Callback: func(e stmtflow.SessionEvent) {
			switch p := e.Payload.(type) {
			case stmtflow.Invoke:
				fmt.Printf("/* %s */ %s;\n", e.Sess, p.Stmt.SQL)
			case stmtflow.Return:
				if p.Err == nil {
					if opts.verbose && !p.Res.IsExecResult() {
						buf, fst := new(bytes.Buffer), true
						p.Res.PrettyPrint(buf)
						for {
							line, err := buf.ReadString('\n')
							if err != nil {
								break
							}
							if fst {
								fmt.Print("-- ", e.Sess, " >> ", line)
								fst = false
							} else {
								fmt.Print("-- ", e.Sess, "    ", line)
							}
						}
					} else {
						fmt.Printf("-- %s >> %s\n", e.Sess, p.Res.String())
					}
					if opts.withLat {
						fmt.Printf("-- %s    %s ~ %s (cost %s)\n", e.Sess,
							p.T[0].Format("15:04:05.000"), p.T[1].Format("15:04:05.000"), p.T[1].Sub(p.T[0]))
					}
				} else {
					fmt.Printf("-- %s >> %s\n", e.Sess, p.Err.Error())
				}
			case stmtflow.Block:
				fmt.Printf("-- %s >> blocked\n", e.Sess)
			case stmtflow.Resume:
				fmt.Printf("-- %s >> resumed\n", e.Sess)
			}
		},
	})
	if w != nil {
		w.Wait()
	}
	if err != nil {
		Throw(err)
	}
}
