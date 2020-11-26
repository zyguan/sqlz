package command

import (
	"context"
	"database/sql"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/zyguan/sqlz/stmtflow"

	_ "github.com/go-sql-driver/mysql"
)

type CommonOptions struct {
	DSN       string
	Timeout   time.Duration
	PingTime  time.Duration
	BlockTime time.Duration
}

func (c *CommonOptions) OpenDB() (*sql.DB, error) {
	return sql.Open("mysql", c.DSN)
}

func (c *CommonOptions) EvalOptions() stmtflow.EvalOptions {
	return stmtflow.EvalOptions{PingTime: c.PingTime, BlockTime: c.BlockTime}
}

func (c *CommonOptions) WithTimeout(ctx context.Context) context.Context {
	if c.Timeout > 0 {
		ctx, _ = context.WithTimeout(ctx, c.Timeout)
	}
	return ctx
}

func Root() *cobra.Command {
	var opts CommonOptions
	cmd := &cobra.Command{
		Use:   "stmtflow",
		Short: "stmtflow - an enhanced mysql-test.",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			if dsn := os.Getenv("STMTFLOW_DSN"); len(dsn) > 0 {
				opts.DSN = dsn
			}
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}
	cmd.PersistentFlags().StringVar(&opts.DSN, "dsn", "root:@tcp(127.0.0.1:3306)/test", "data source name")
	cmd.PersistentFlags().DurationVar(&opts.Timeout, "timeout", 60*time.Second, "timeout for a single test")
	cmd.PersistentFlags().DurationVar(&opts.PingTime, "ping-time", 200*time.Millisecond, "max wait time to ping a blocked statement")
	cmd.PersistentFlags().DurationVar(&opts.BlockTime, "block-time", 3*time.Second, "max time to wait a newly submitted statement")

	cmd.AddCommand(AutoGen(), Play(&opts), Test(&opts))

	return cmd
}
