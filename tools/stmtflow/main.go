package main

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/zyguan/sqlz/tools/stmtflow/command"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func main() {
	cmd := command.Root()
	if err := cmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "\x1b[0;31mError: %+v\x1b[0m\n", err)
		os.Exit(1)
	}
}
