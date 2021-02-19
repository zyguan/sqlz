package command

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
	"github.com/zyguan/sqlz/stmtflow"
	"github.com/zyguan/sqlz/tools/stmtflow/core"
)

func Play(c *CommonOptions) *cobra.Command {
	var opts struct {
		stmtflow.TextDumpOptions
		Write bool
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
			for _, path := range args {
				fmt.Println("# " + path)
				var (
					in       io.ReadCloser
					result   stmtflow.History
					done     func()
					evalOpts = c.EvalOptions()
				)
				db, err := c.OpenDB()
				if err != nil {
					return err
				}
				in, err = os.Open(path)
				if err != nil {
					return err
				}
				if opts.Write {
					textOut, err := os.OpenFile(resultPathForText(path), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
					if err != nil {
						return err
					}
					jsonOut, err := os.OpenFile(resultPathForJson(path), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
					if err != nil {
						return err
					}
					textWriter := stmtflow.TextDumper(io.MultiWriter(os.Stdout, textOut), opts.TextDumpOptions)
					evalOpts.Callback = stmtflow.ComposeHandler(result.Collect, textWriter)
					done = func() {
						result.DumpJson(jsonOut, stmtflow.JsonDumpOptions{})
						jsonOut.Close()
						textOut.Close()
						in.Close()
						db.Close()
					}
				} else {
					evalOpts.Callback = stmtflow.TextDumper(os.Stdout, opts.TextDumpOptions)
					done = func() {
						in.Close()
						db.Close()
					}
				}

				if err = stmtflow.Run(c.WithTimeout(ctx), db, core.ParseSQL(in), evalOpts); err != nil {
					return err
				}

				done()
			}
			return nil
		},
	}
	cmd.Flags().BoolVarP(&opts.Write, "write", "w", false, "write to expected result files")
	cmd.Flags().BoolVarP(&opts.Verbose, "verbose", "v", true, "verbose output")
	cmd.Flags().BoolVar(&opts.WithLat, "with-lat", false, "record latency of each statement")
	return cmd
}
