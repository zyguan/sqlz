package command

import (
	"context"
	"fmt"
	"log"

	"github.com/spf13/cobra"
	"github.com/zyguan/sqlz/stmtflow"
	"github.com/zyguan/sqlz/tools/stmtflow/test"
)

func Test(c *CommonOptions) *cobra.Command {
	// TODO: filter & dry run
	cmd := &cobra.Command{
		Use:           "test [suite.jsonnet ...]",
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
			// TODO: support concurrent execution
			errCnt := 0
			for _, path := range args {
				log.Printf("load %s", path)
				s, err := test.Load(path)
				if err != nil {
					return err
				}
				for _, t := range s.Tests {
					var h stmtflow.History
					evalOpts := c.EvalOptions()
					evalOpts.Callback = h.Collect
					w, err := stmtflow.Eval(c.WithTimeout(ctx), db, t.Flow, evalOpts)
					if w != nil {
						w.Wait()
					}
					if err != nil {
						log.Printf("[%s#%s] eval failed: %+v", path, t.Name, err)
						errCnt += 1
						continue
					}
					if err = t.Assert(h); err != nil {
						log.Printf("[%s#%s] assert failed: %+v", path, t.Name, err)
						errCnt += 1
						continue
					}
					log.Printf("[%s#%s] passed", path, t.Name)
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

	return cmd
}
