package command

import (
	"strings"
)

const (
	sqlExt        = ".sql"
	stdTestExt    = ".t.sql"
	stdTextResExt = ".r.sql"
	stdJsonResExt = ".r.json"
	genTestsFile  = "_generated_tests.libsonnet"
)

func splitTestExt(path string) (string, string) {
	if strings.HasSuffix(path, stdTestExt) {
		return path[0 : len(path)-len(stdTestExt)], stdTestExt
	} else if strings.HasSuffix(path, sqlExt) {
		return path[0 : len(path)-len(sqlExt)], sqlExt
	}
	return path, ""
}

func resultPathForText(path string) string {
	base, _ := splitTestExt(path)
	return base + stdTextResExt
}

func resultPathForJson(path string) string {
	base, _ := splitTestExt(path)
	return base + stdJsonResExt
}
