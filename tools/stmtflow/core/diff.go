package core

import (
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func LocalDiff(w io.Writer, name string, txt1 string, txt2 string, color bool) error {
	name = strings.ReplaceAll(name, string(filepath.Separator), "__")
	dir, err := ioutil.TempDir("", name+".")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	args := []string{"-u", "-N"}
	if color {
		args = append(args, "--color")
	}
	path1 := filepath.Join(dir, "a")
	if err = ioutil.WriteFile(path1, []byte(txt1), 0644); err != nil {
		return err
	}
	args = append(args, path1)

	path2 := filepath.Join(dir, "b")
	if err = ioutil.WriteFile(path2, []byte(txt2), 0644); err != nil {
		return err
	}
	args = append(args, path2)
	cmd := exec.Command("diff", args...)
	cmd.Stdout = w
	return cmd.Run()
}
