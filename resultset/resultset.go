package resultset

import (
	"bytes"
	"compress/gzip"
	"crypto/sha1"
	"database/sql"
	"encoding/binary"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"io"
	"sort"
	"strconv"

	"github.com/olekukonko/tablewriter"
)

type Rows [][]interface{}

type ColumnDef struct {
	Name      string
	Type      string
	Length    int64
	Precision int64
	Scale     int64
	Nullable  bool

	HasNullable       bool
	HasLength         bool
	HasPrecisionScale bool
}

type ExecResult struct {
	RowsAffected int64
	LastInsertId int64

	HasRowsAffected bool
	HasLastInsertId bool
}

type ResultSet struct {
	cols []ColumnDef
	data [][][]byte
	nils []uint64
	exec ExecResult
}

func New(schema []ColumnDef) *ResultSet {
	return &ResultSet{cols: schema}
}

func NewFromResult(res sql.Result) *ResultSet {
	var err error
	rs := &ResultSet{exec: ExecResult{}}
	rs.exec.RowsAffected, err = res.RowsAffected()
	rs.exec.HasRowsAffected = err == nil
	rs.exec.LastInsertId, err = res.LastInsertId()
	rs.exec.HasLastInsertId = err == nil
	return rs
}

func ReadFromRows(rows *sql.Rows) (*ResultSet, error) {
	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	cols := make([]ColumnDef, len(types))
	for i, t := range types {
		cols[i].Name = t.Name()
		cols[i].Type = t.DatabaseTypeName()
		cols[i].Nullable, cols[i].HasNullable = t.Nullable()
		cols[i].Length, cols[i].HasLength = t.Length()
		cols[i].Precision, cols[i].Scale, cols[i].HasPrecisionScale = t.DecimalSize()
	}
	rs, i := New(cols), 0
	for rows.Next() {
		row := rs.AllocateRow()
		if err = rows.Scan(row...); err != nil {
			return rs, err
		}
		for j, col := range row {
			if *col.(*[]byte) == nil {
				rs.markNil(i, j)
			}
		}
		i += 1
	}
	return rs, rows.Err()
}

func (rs *ResultSet) String() string {
	if rs.IsExecResult() {
		return strconv.FormatInt(rs.ExecResult().RowsAffected, 10) + " rows affected"
	}
	if rs.NRows() == 0 {
		return "empty set"
	}
	return strconv.Itoa(rs.NRows()) + " rows in set"
}

func (rs *ResultSet) IsExecResult() bool { return len(rs.cols) == 0 }

func (rs *ResultSet) ExecResult() ExecResult { return rs.exec }

func (rs *ResultSet) NRows() int { return len(rs.data) }

func (rs *ResultSet) NCols() int { return len(rs.cols) }

func (rs *ResultSet) ColumnDef(i int) ColumnDef {
	if i < 0 {
		i += len(rs.cols)
	}
	if i < 0 || i >= len(rs.cols) {
		return ColumnDef{}
	}
	return rs.cols[i]
}

func (rs *ResultSet) Sort(less func(r1 int, r2 int) bool) { sort.SliceStable(rs.data, less) }

func (rs *ResultSet) RawValue(i int, j int) ([]byte, bool) {
	if i < 0 {
		i += len(rs.data)
	}
	if i < 0 || i >= len(rs.data) {
		return nil, false
	}
	row := rs.data[i]
	if j < 0 {
		j += len(row)
	}
	if j < 0 || j >= len(row) {
		return nil, false
	}
	v := rs.data[i][j]
	if v == nil && !rs.isNil(i, j) {
		return []byte{}, true
	}
	return v, true
}

func (rs *ResultSet) AllocateRow() []interface{} {
	if rs.IsExecResult() {
		return nil
	}
	row := make([][]byte, len(rs.cols))
	rs.data = append(rs.data, row)
	xs := make([]interface{}, len(row))
	for i := range row {
		xs[i] = &row[i]
	}
	return xs
}

func (rs *ResultSet) DataDigest(opts DigestOptions) string {
	if rs.IsExecResult() {
		return ""
	}
	h := sha1.New()
	for i, row := range rs.data {
	cellLoop:
		for j, v := range row {
			for _, filter := range opts.Filters {
				if filter != nil && !filter(i, j, v, rs.cols[j]) {
					continue cellLoop
				}
			}
			_ = rs.encodeCellTo(h, i, j, opts.Mapper)
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (rs *ResultSet) OrderedDigest(opts DigestOptions) string {
	digests := make([][]byte, rs.NRows())
	for i, row := range rs.data {
		h := sha1.New()
	cellLoop:
		for j, v := range row {
			for _, filter := range opts.Filters {
				if filter != nil && !filter(i, j, v, rs.cols[j]) {
					continue cellLoop
				}
			}
			_ = rs.encodeCellTo(h, i, j, opts.Mapper)
		}
		digests[i] = h.Sum(nil)
	}
	sort.Slice(digests, func(i, j int) bool {
		return bytes.Compare(digests[i], digests[j]) < 0
	})
	h := sha1.New()
	for _, digest := range digests {
		h.Write(digest)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (rs *ResultSet) AssertData(expect Rows, onErr ...func(act *ResultSet, exp Rows, err error)) (err error) {
	defer func() {
		if err != nil {
			for _, cb := range onErr {
				cb(rs, expect, err)
			}
		}
	}()
	if len(expect) != rs.NRows() {
		err = fmt.Errorf("row count mismatch: %d <> %d", rs.NRows(), len(expect))
		return
	}
	for i, row := range rs.data {
		if len(expect[i]) != rs.NCols() {
			err = fmt.Errorf("invalid expected data: there are %d cols at %d row", len(expect[i]), i)
			return
		}
		for j, exp := range expect[i] {
			if isNil := rs.isNil(i, j); exp == nil && isNil {
				continue
			} else if exp == nil {
				err = fmt.Errorf("data mismatch (%q#%d): expect <nil> but got %v", rs.cols[j].Name, i, row[j])
				return
			} else if isNil {
				err = fmt.Errorf("data mismatch (%q#%d): expect %v but got <nil>", rs.cols[j].Name, i, exp)
				return
			}

			ok := false
			act, _ := rs.RawValue(i, j)
			switch y := exp.(type) {
			case string:
				ok = string(act) == y
			case fmt.Stringer:
				ok = string(act) == y.String()
			case []byte:
				ok = bytes.Compare(act, y) == 0
			case Bin:
				ok = bytes.Compare(act, y.Bytes()) == 0
			default:
				ok = string(act) == fmt.Sprintf("%v", y)
			}
			if !ok {
				err = fmt.Errorf("data mismatch (%q#%d): %v <> %v", rs.cols[j].Name, i, act, exp)
				return
			}
		}
	}
	return
}

func (rs *ResultSet) PrettyPrint(out io.Writer) {
	table := tablewriter.NewWriter(out)
	table.SetAutoWrapText(false)
	if rs.IsExecResult() {
		table.SetHeader([]string{"RowsAffected", "LastInsertId"})
		row := []string{"NULL", "NULL"}
		if rs.exec.HasRowsAffected {
			row[0] = strconv.FormatInt(rs.exec.RowsAffected, 10)
		}
		if rs.exec.HasLastInsertId {
			row[1] = strconv.FormatInt(rs.exec.LastInsertId, 10)
		}
		table.Append(row)
		table.Render()
		return
	}
	hdr := make([]string, len(rs.cols))
	for i, c := range rs.cols {
		hdr[i] = c.Name
	}
	table.SetHeader(hdr)
	for i, r := range rs.data {
		row := make([]string, len(r))
		for j, s := range r {
			if rs.isNil(i, j) {
				row[j] = "NULL"
			} else {
				row[j] = string(s)
			}
		}
		table.Append(row)
	}
	table.Render()
}

func (rs *ResultSet) Encode() ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := rs.EncodeTo(buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (rs *ResultSet) EncodeTo(w io.Writer) error {
	zw := gzip.NewWriter(w)
	defer zw.Close()
	enc := gob.NewEncoder(zw)
	tmp := struct {
		Cols []ColumnDef
		Data [][][]byte
		Nils []uint64
		Exec ExecResult
	}{rs.cols, rs.data, rs.nils, rs.exec}
	return enc.Encode(tmp)
}

func (rs *ResultSet) Decode(raw []byte) error {
	return rs.DecodeFrom(bytes.NewReader(raw))
}

func (rs *ResultSet) DecodeFrom(r io.Reader) error {
	zr, err := gzip.NewReader(r)
	if err != nil {
		return err
	}
	dec := gob.NewDecoder(zr)
	var tmp struct {
		Cols []ColumnDef
		Data [][][]byte
		Nils []uint64
		Exec ExecResult
	}
	if err := dec.Decode(&tmp); err != nil {
		return err
	}
	rs.cols, rs.data, rs.nils, rs.exec = tmp.Cols, tmp.Data, tmp.Nils, tmp.Exec
	return nil
}

func (rs *ResultSet) markNil(i int, j int) {
	n := i*len(rs.cols) + j
	for 64*len(rs.nils) <= n {
		rs.nils = append(rs.nils, 0)
	}
	pos, off := n/64, n%64
	rs.nils[pos] |= 1 << off
}

func (rs *ResultSet) isNil(i int, j int) bool {
	n := i*len(rs.cols) + j
	if 64*len(rs.nils) <= n {
		return false
	}
	pos, off := n/64, n%64
	return (rs.nils[pos] & (1 << off)) > 0
}

func (rs *ResultSet) encodeCellTo(w io.Writer, i int, j int, f func(i int, j int, raw []byte, def ColumnDef) []byte) error {
	buf := make([]byte, 4)
	raw := rs.data[i][j]
	if f != nil {
		raw = f(i, j, raw, rs.cols[j])
	}
	binary.BigEndian.PutUint32(buf, uint32(len(raw)))
	if rs.isNil(i, j) {
		buf[0] |= 0x80
	}
	if _, err := w.Write(buf); err != nil {
		return err
	}
	if _, err := w.Write(raw); err != nil {
		return err
	}
	return nil
}

type DigestOptions struct {
	Filters []func(i int, j int, raw []byte, def ColumnDef) bool
	Mapper  func(i int, j int, raw []byte, def ColumnDef) []byte
}

type Bin interface {
	Bytes() []byte
}

type BinBool bool

func (b BinBool) Bytes() []byte {
	if b {
		return []byte{1}
	}
	return []byte{0}
}

// TODO: impl other types for binary protocol
