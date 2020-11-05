package command

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/zyguan/sqlz/stmtflow"
	"github.com/zyguan/sqlz/tools/stmtflow/format"
)

func Play(c *CommonOptions) *cobra.Command {
	var opts struct {
		format.TextDumpOptions
		Write    bool
		Negative bool
	}
	cmd := &cobra.Command{
		Use:           "play [test.sql ...]",
		Short:         "Try tests",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return cmd.Help()
			}
			ctx := context.Background()
			db, err := c.OpenDB()
			if err != nil {
				return err
			}
			defer db.Close()
			for _, path := range args {
				fmt.Println("# " + path)

				sqlIn, textOut, jsonOut, err := openPlayFiles(path, opts.Write, opts.Negative)
				if err != nil {
					return err
				}

				var (
					history  = new(stmtflow.History)
					stmts    = format.ParseSQL(sqlIn)
					evalOpts = c.EvalOptions()
				)

				if opts.Write {
					evalOpts.Callback = stmtflow.ComposeHandler(format.TextEventDumper(io.MultiWriter(textOut, os.Stdout), opts.TextDumpOptions), history.Collect)
				} else {
					evalOpts.Callback = format.TextEventDumper(textOut, opts.TextDumpOptions)
				}

				w, err := stmtflow.Eval(c.WithTimeout(ctx), db, stmts, evalOpts)
				if w != nil {
					w.Wait()
				}
				if err != nil {
					return err
				}

				if opts.Write {
					if err = history.DumpJson(jsonOut); err != nil {
						return err
					}
					textOut.Close()
					jsonOut.Close()
				}

				sqlIn.Close()
			}
			return nil
		},
	}
	cmd.Flags().BoolVarP(&opts.Write, "write", "w", false, "write to expected result files")
	cmd.Flags().BoolVarP(&opts.Negative, "negative", "n", false, "mark as a negative test")
	cmd.Flags().BoolVarP(&opts.Verbose, "verbose", "v", true, "verbose output")
	cmd.Flags().BoolVar(&opts.WithLat, "with-lat", false, "record latency of each statement")
	return cmd
}

func openPlayFiles(path string, write bool, negative bool) (io.ReadCloser, io.WriteCloser, io.WriteCloser, error) {
	r, err := os.Open(path)
	if err != nil {
		return nil, nil, nil, err
	}
	if !write {
		return r, os.Stdout, nil, nil
	}
	ext := filepath.Ext(path)
	base := path[0 : len(path)-len(ext)]
	if negative {
		base += ".neg"
	}
	textPath, jsonPath := base+".result.out", base+".result.json"
	f1, err := os.OpenFile(textPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		r.Close()
		return nil, nil, nil, err
	}
	f2, err := os.OpenFile(jsonPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		r.Close()
		f1.Close()
		return nil, nil, nil, err
	}
	return r, f1, f2, nil
}
