package core

import (
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func LocalDiff(w io.Writer, name string, txt1 string, txt2 string, diff []string) error {
	if len(diff) == 0 {
		diff = []string{"diff", "-u", "-N", "--color"}
	}
	name = strings.ReplaceAll(name, string(filepath.Separator), "__")
	dir, err := ioutil.TempDir("", name+".")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	path1 := filepath.Join(dir, "a")
	if err = ioutil.WriteFile(path1, []byte(txt1), 0644); err != nil {
		return err
	}
	diff = append(diff, path1)

	path2 := filepath.Join(dir, "b")
	if err = ioutil.WriteFile(path2, []byte(txt2), 0644); err != nil {
		return err
	}
	diff = append(diff, path2)

	cmd := exec.Command(diff[0], diff[1:]...)
	cmd.Stdout = w
	return cmd.Run()
}
