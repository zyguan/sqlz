package sqlz

import (
	"context"
	"strings"
)

type BulkInsert struct {
	Prefix string
	Suffix string
	Row    string
	Sep    string

	ex   ExecerContext
	stmt string
	size int
	rows int
	args []interface{}
}

func (bi *BulkInsert) Init(ex ExecerContext, size int) {
	if len(bi.Sep) == 0 {
		bi.Sep = ", "
	}
	if size < 1 {
		size = 1
	}
	bi.ex = ex
	bi.stmt = bi.sql(size)
	bi.size = size
	bi.rows = 0
	bi.args = []interface{}{}
}

func (bi *BulkInsert) Next(ctx context.Context, args ...interface{}) (err error) {
	bi.rows, bi.args = bi.rows+1, append(bi.args, args...)
	if bi.rows < bi.size {
		return nil
	}
	if bi.rows == bi.size {
		_, err = bi.ex.ExecContext(ctx, bi.stmt, bi.args...)
	} else {
		_, err = bi.ex.ExecContext(ctx, bi.sql(bi.rows), bi.args...)
	}
	if err == nil {
		bi.rows, bi.args = 0, bi.args[:0]
	}
	return
}

func (bi *BulkInsert) Done(ctx context.Context) (err error) {
	if bi.rows == 0 {
		return nil
	}
	_, err = bi.ex.ExecContext(ctx, bi.sql(bi.rows), bi.args...)
	if err == nil {
		bi.rows, bi.args = 0, bi.args[:0]
	}
	return
}

func (bi *BulkInsert) SQL() (string, []interface{}) {
	return bi.sql(bi.rows), bi.args
}

func (bi *BulkInsert) sql(rows int) string {
	if rows < 1 {
		return ""
	}
	var buf strings.Builder
	buf.Grow(len(bi.Prefix) + len(bi.Suffix) + len(bi.Row)*bi.rows + len(bi.Sep)*(bi.rows-1))
	buf.WriteString(bi.Prefix)
	for i := 0; i < rows; i++ {
		if i > 0 {
			buf.WriteString(bi.Sep)
		}
		buf.WriteString(bi.Row)
	}
	buf.WriteString(bi.Suffix)
	return buf.String()
}
