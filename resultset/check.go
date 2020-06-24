package resultset

import (
	"bytes"
	"fmt"
)

type ValueChecker interface {
	Match(row int, col int, def ColumnDef) bool
	Equal(v1 []byte, v2 []byte, def ColumnDef) bool
}

type RawBytesChecker struct{}

func (c RawBytesChecker) Match(row int, col int, def ColumnDef) bool { return true }

func (c RawBytesChecker) Equal(v1 []byte, v2 []byte, def ColumnDef) bool { return bytes.Equal(v1, v2) }

type DiffOptions struct {
	CheckSchema    bool
	CheckPrecision bool
	ValueCheckers  []ValueChecker
}

func Diff(rs1 *ResultSet, rs2 *ResultSet, opts DiffOptions) error {
	if rs1.IsExecResult() != rs2.IsExecResult() {
		return fmt.Errorf("result type mismatch: %s <> %s", rs1.String(), rs2.String())
	}
	if rs1.IsExecResult() {
		if rs1.exec != rs2.exec {
			return fmt.Errorf("execute result mismatch: %v <> %v", rs1.exec, rs2.exec)
		}
		return nil
	}
	if rs1.NCols() != rs2.NCols() {
		return fmt.Errorf("col count mismatch: %d <> %d", rs1.NCols(), rs2.NCols())
	}
	if rs1.NRows() != rs2.NRows() {
		return fmt.Errorf("row count mismatch: %d <> %d", rs1.NRows(), rs2.NRows())
	}
	if opts.CheckSchema {
		schemaDiff := diffSchema(rs1.cols, rs2.cols, opts)
		if len(schemaDiff) > 0 {
			return fmt.Errorf("schema mismatch: " + schemaDiff)
		}
	}
	checkers := opts.ValueCheckers
	if checkers == nil {
		checkers = []ValueChecker{RawBytesChecker{}}
	}

	for i := 0; i < rs1.NRows(); i++ {
		for j := 0; j < rs1.NCols(); j++ {
			v1, _ := rs1.RawValue(i, j)
			v2, _ := rs2.RawValue(i, j)
			for _, checker := range checkers {
				if checker.Match(i, j, rs1.cols[j]) {
					if !checker.Equal(v1, v2, rs1.cols[j]) {
						return fmt.Errorf("data mismatch (%q#%d): %v <> %v", rs1.cols[j].Name, i, v1, v2)
					}
					break
				}
			}
		}
	}
	return nil
}

func diffSchema(cols1 []ColumnDef, cols2 []ColumnDef, opts DiffOptions) string {
	for i := range cols1 {
		t1, t2 := cols1[i], cols2[i]
		if t1.Name != t2.Name {
			return fmt.Sprintf("cols[%d].name: %s <> %s", i, t1.Name, t2.Name)
		}
		if t1.Type != t2.Type {
			return fmt.Sprintf("cols[%d].type: %s <> %s", i, t1.Type, t2.Type)
		}
		if t1.HasNullable != t2.HasNullable || t1.Nullable != t2.Nullable {
			return fmt.Sprintf("cols[%d].nullable: %v <> %v", i, t1.Nullable, t2.Nullable)
		}

		if t1.HasLength != t2.HasLength || t1.Length != t2.Length {
			return fmt.Sprintf("cols[%d].type: %s(%d) <> %s(%d)", i, t1.Type, t1.Length, t2.Type, t2.Length)
		}
		if opts.CheckPrecision {
			if t1.HasPrecisionScale != t2.HasPrecisionScale || t1.Precision != t2.Precision || t1.Scale != t2.Scale {
				return fmt.Sprintf("cols[%d].type: %s(%d,%d) <> %s(%d,%d)", i,
					t1.Type, t1.Precision, t1.Scale, t2.Type, t2.Precision, t2.Scale)
			}
		}
	}
	return ""
}
