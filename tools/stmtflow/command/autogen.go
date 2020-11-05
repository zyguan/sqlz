package command

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
)

func AutoGen() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "gen <dir>",
		Short:         "Generate test manifest",
		SilenceErrors: true,
		Args:          cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Println("[")
			defer fmt.Println("]")
			return filepath.Walk(args[0], func(path string, info os.FileInfo, err error) error {
				if err != nil || info.IsDir() || !strings.HasSuffix(path, ".result.json") {
					return nil
				}
				neg := strings.HasSuffix(path, ".neg.result.json")
				name := strings.TrimSuffix(strings.TrimSuffix(path, ".result.json"), ".neg")
				sqlFrom := strings.TrimPrefix(name, args[0])
				resFrom := sqlFrom
				if neg {
					resFrom += ".neg"
				}
				if fi, err := os.Stat(name + ".sql"); err != nil || fi.IsDir() {
					return nil
				}
				fmt.Printf(`  { name: "%s", flow: std.native("parseSQL")(importstr "%s.sql"), expect: import "%s.result.json", negative: %v },`+"\n", name, sqlFrom, resFrom, neg)
				return nil
			})
		},
	}
	return cmd
}
