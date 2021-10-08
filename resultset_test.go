package sqlz

import (
	"database/sql"
	"encoding/base64"
	"flag"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/go-sql-driver/mysql"
)

var opts struct {
	mysqlDSN string
}

func init() {
	flag.StringVar(&opts.mysqlDSN, "mysql-dsn", "root:@tcp(127.0.0.1:4000)/information_schema", "mysql dsn")
}

func testDB(t *testing.T) *sql.DB {
	db, err := sql.Open("mysql", opts.mysqlDSN)
	assert.NoError(t, err)
	if err := db.Ping(); err != nil {
		t.Skipf("failed to ping target mysql: dsn=%s, err=%v", opts.mysqlDSN, err)
	}
	return db
}

var rss = []ResultSet{
	{nil, nil, nil, ExecResult{0, 0, false, false}},
	{[]ColumnDef{}, nil, nil, ExecResult{1, 0, true, false}},
	{[]ColumnDef{
		{Name: "foo", Type: "TEXT"},
	}, nil, nil, ExecResult{0, 1, false, true}},
	{[]ColumnDef{
		{Name: "foo", Type: "TEXT"},
	}, [][][]byte{
		{{0x1}},
		{nil},
		{{}},
	}, []uint64{2}, ExecResult{1, 1, true, true}},
}

func TestAssertDataNil(t *testing.T) {
	assert := assert.New(t)

	callCnt := 0
	cb := func(_ *ResultSet, _ Rows, err error) {
		callCnt += 1
		assert.Contains(err.Error(), "<nil>")
	}

	// nil can match nil & empty string can match empty string
	rs := ResultSet{cols: []ColumnDef{{Name: "foo", Type: "TEXT"}}, data: [][][]byte{{nil}}}
	rs.markNil(0, 0)
	assert.NoError(rs.AssertData(Rows{{nil}}, cb))
	assert.Equal(0, callCnt)
	rs = ResultSet{cols: []ColumnDef{{Name: "foo", Type: "TEXT"}}, data: [][][]byte{{[]byte{}}}}
	assert.NoError(rs.AssertData(Rows{{""}}, cb))
	assert.Equal(0, callCnt)

	// nil can't match empty string
	rs = ResultSet{cols: []ColumnDef{{Name: "foo", Type: "TEXT"}}, data: [][][]byte{{[]byte{}}}}
	assert.Error(rs.AssertData(Rows{{nil}}, cb))
	assert.Equal(1, callCnt)
	rs = ResultSet{cols: []ColumnDef{{Name: "foo", Type: "TEXT"}}, data: [][][]byte{{nil}}}
	rs.markNil(0, 0)
	assert.Error(rs.AssertData(Rows{{""}}, cb))
	assert.Equal(2, callCnt)
}

func TestAssertDataFloat(t *testing.T) {
	rs := ResultSet{
		cols: []ColumnDef{{Name: "foo", Type: "FLOAT"}},
		data: [][][]byte{
			{[]byte("2.7180")},
			{[]byte("3.1400")},
		},
	}
	require.NoError(t, rs.AssertData(Rows{{Float(2.718)}, {Float(3.14)}}))
	require.NoError(t, rs.AssertData(Rows{{Float(2.72, 0.01)}, {Float(3.15, 0.01)}}))
	require.Error(t, rs.AssertData(Rows{{Float(2.72, 0.001)}, {Float(3.15, 0.01)}}))
	require.Error(t, rs.AssertData(Rows{{Float(2.72, 0.01)}, {Float(3.15, 0.001)}}))
}

func TestDataDigest(t *testing.T) {
	rs1 := ResultSet{
		cols: []ColumnDef{{Name: "foo", Type: "FLOAT"}},
		data: [][][]byte{
			{[]byte("2.718")},
			{[]byte("3.14")},
		},
	}
	rs2 := ResultSet{
		cols: []ColumnDef{{Name: "foo", Type: "FLOAT"}},
		data: [][][]byte{
			{[]byte("3.141")},
			{[]byte("2.72")},
		},
	}
	rs3 := ResultSet{
		cols: []ColumnDef{{Name: "foo", Type: "FLOAT"}},
		data: [][][]byte{
			{[]byte("2.7180")},
			{[]byte("3.1400")},
		},
	}
	opts1 := DigestOptions{}
	opts2 := DigestOptions{Sort: true}
	require.False(t, rs1.DataDigest(opts1) == rs2.DataDigest(opts1))
	require.False(t, rs1.DataDigest(opts1) == rs3.DataDigest(opts2))
	require.False(t, rs1.DataDigest(opts2) == rs2.DataDigest(opts2))

	opts1.Mapper = func(i int, j int, raw []byte, def ColumnDef) []byte {
		if def.Type != "FLOAT" {
			return raw
		}
		f, err := strconv.ParseFloat(string(raw), 10)
		if err != nil {
			return raw
		}
		return []byte(fmt.Sprintf("%.2f", f))
	}
	opts2.Mapper = opts1.Mapper
	require.False(t, rs1.DataDigest(opts1) == rs2.DataDigest(opts1))
	require.True(t, rs1.DataDigest(opts1) == rs3.DataDigest(opts1))
	require.True(t, rs1.DataDigest(opts2) == rs2.DataDigest(opts2))

	opts1.Mapper = func(i int, j int, raw []byte, def ColumnDef) []byte {
		if def.Type != "FLOAT" {
			return raw
		}
		f, err := strconv.ParseFloat(string(raw), 10)
		if err != nil {
			return raw
		}
		return []byte(fmt.Sprintf("%.6f", f))
	}
	require.True(t, rs1.DataDigest(opts1) == rs3.DataDigest(opts1))
	require.True(t, rs1.DataDigest(opts2) == rs2.DataDigest(opts2))

	opts2.Filter = func(i int, j int, raw []byte, def ColumnDef) bool {
		return i < 1
	}
	require.False(t, rs1.DataDigest(opts2) == rs2.DataDigest(opts2))
}

func TestEncodeDecodeCheck(t *testing.T) {
	for i, rs := range rss {
		t.Run("EncodeDecodeCheck#"+strconv.Itoa(i), tEncodeDecodeCheck(&rs))
	}
}

func TestEncodeDecodeWithMySQLDataSource(t *testing.T) {
	db := testDB(t)
	defer db.Close()
	for _, table := range []string{
		"INFORMATION_SCHEMA.CHARACTER_SETS",
		"INFORMATION_SCHEMA.COLLATIONS",
		"INFORMATION_SCHEMA.TABLES",
		"INFORMATION_SCHEMA.COLUMNS",
	} {
		rows, err := db.Query("SELECT * FROM " + table)
		assert.NoError(t, err, "select "+table)
		rs, err := ReadFromRows(rows)
		assert.NoError(t, err, "read rows from "+table)
		t.Run("EncodeDecode[MySQL:"+table+"]", tEncodeDecodeCheck(rs))
	}
}

func TestFloatCell(t *testing.T) {
	type EqTest struct {
		raw string
		ok  bool
	}
	for i, tt := range []struct {
		float   FloatCell
		literal string
		tests   []EqTest
	}{
		{float: Float(), literal: "0", tests: []EqTest{
			{"0", true},
			{"0.0", true},
			{"0.00001", false},
			{"NULL", false},
		}},
		{float: Float(3.14), literal: "3.14", tests: []EqTest{
			{"3.14", true},
			{"3.1400", true},
			{"3.1415", false},
			{"NULL", false},
		}},
		{float: Float(3.1415, 0.001), literal: "3.1415±0.001", tests: []EqTest{
			{"3.1415", true},
			{"3.1410", true},
			{"3.1420", true},
			{"3.14", false},
			{"NULL", false},
		}},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			require.Equal(t, tt.literal, tt.float.String())
			require.Equal(t, tt.literal, fmt.Sprintf("%s", tt.float))
			require.Equal(t, tt.literal, fmt.Sprintf("%v", tt.float))
			require.Equal(t, tt.literal, fmt.Sprintf("%+v", tt.float))
			for _, et := range tt.tests {
				require.Equal(t, et.ok, tt.float.EqualTo(ColumnDef{}, []byte(et.raw)))
			}
		})
	}
}

func tEncodeDecodeCheck(rs1 *ResultSet) func(t *testing.T) {
	return func(t *testing.T) {
		bs, err := rs1.Encode()
		t.Log(">>", "\""+base64.StdEncoding.EncodeToString(bs)+"\",")
		assert.NoError(t, err)
		rs2 := &ResultSet{}
		assert.NoError(t, rs2.Decode(bs))
		assert.Equal(t, rs1.DataDigest(DigestOptions{}), rs2.DataDigest(DigestOptions{}))
		assert.Equal(t, rs1.ExecResult(), rs2.ExecResult())
		assert.NoError(t, Diff(rs1, rs2, DiffOptions{CheckPrecision: true, CheckSchema: true}))

		for i := 0; i < rs1.NCols(); i++ {
			assert.Equal(t, rs1.ColumnDef(i), rs2.ColumnDef(i))
		}
	}
}
