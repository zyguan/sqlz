package command

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/pingcap/errors"
	"github.com/spf13/cobra"
	"github.com/zyguan/sqlz/stmtflow"
	"github.com/zyguan/sqlz/tools/stmtflow/core"
)

type testOptions struct {
	stmtflow.EvalOptions
	Filter string
	DryRun bool
	Diff   bool
}

func Test(c *CommonOptions) *cobra.Command {
	opts := testOptions{EvalOptions: c.EvalOptions()}
	cmd := &cobra.Command{
		Use:           "test [tests.jsonnet ...]",
		Short:         "Run tests",
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
			errCnt := 0
			for _, path := range args {
				log.Printf("[%s] load tests", path)
				tests, err := core.Load(path, opts.Filter)
				if err != nil {
					return err
				}
				// TODO: support concurrent execution
				for _, t := range tests {
					if opts.DryRun {
						log.Printf("[%s#%s] type:%s labels:%s", path, t.Name, t.AssertMethod, t.Labels)
						continue
					}
					for i := 0; i < t.Repeat; i++ {
						err = testOne(c.WithTimeout(ctx), db, t, opts)
						if err != nil {
							break
						}
					}
					if err != nil {
						log.Printf("[%s#%s] failed: %+v", path, t.Name, err)
						errCnt += 1
					} else {
						log.Printf("[%s#%s] passed", path, t.Name)
					}
				}
			}
			if errCnt > 0 {
				plural := ""
				if errCnt > 1 {
					plural = "s"
				}
				return fmt.Errorf("%d test%s failed", errCnt, plural)
			}
			return nil
		},
	}
	cmd.Flags().StringVarP(&opts.Filter, "filter", "f", "", "filter tests by a jsonnet expr, eg. std.startsWith(test.name, 'foo')")
	cmd.Flags().BoolVarP(&opts.DryRun, "dry-run", "n", false, "just list tests to be run")
	cmd.Flags().BoolVar(&opts.Diff, "diff", false, "diff text output if available")

	return cmd
}

func testOne(ctx context.Context, db *sql.DB, test core.Test, opts testOptions) (err error) {
	var actual stmtflow.History
	evalOpts := opts.EvalOptions
	evalOpts.Callback = actual.Collect
	err = stmtflow.Run(ctx, db, test.Test, evalOpts)
	if err != nil {
		return errors.Annotate(err, "run test")
	}
	err = test.Assert(actual)
	if err == nil || !opts.Diff {
		return
	}
	exp, ok := test.ExpectedText()
	if !ok {
		return
	}
	buf := new(bytes.Buffer)
	if e := actual.DumpText(buf, stmtflow.TextDumpOptions{Verbose: true}); e != nil {
		return
	}
	_ = core.LocalDiff(os.Stdout, test.Name, exp, buf.String(), true)
	return
}
