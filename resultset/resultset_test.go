package resultset

import (
	"database/sql"
	"encoding/base64"
	"flag"
	"strconv"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
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

func tEncodeDecodeCheck(rs1 *ResultSet) func(t *testing.T) {
	return func(t *testing.T) {
		bs, err := rs1.Encode()
		t.Log(">>", "\""+base64.StdEncoding.EncodeToString(bs)+"\",")
		assert.NoError(t, err)
		rs2 := &ResultSet{}
		assert.NoError(t, rs2.Decode(bs))
		assert.Equal(t, rs1.DataDigest(), rs2.DataDigest())
		assert.Equal(t, rs1.ExecResult(), rs2.ExecResult())
		assert.NoError(t, Diff(rs1, rs2, DiffOptions{CheckPrecision: true, CheckSchema: true}))

		for i := 0; i < rs1.NCols(); i++ {
			assert.Equal(t, rs1.ColumnDef(i), rs2.ColumnDef(i))
		}
	}
}
