package binlog

import (
	"os"
	"testing"

	"github.com/LightKool/mysql.go.v1"
	"github.com/juju/errors"
)

func TestDriver(t *testing.T) {
	driver := &mysql.MySQLDriver{}
	dsn := "root:abcd1234@tcp(10.17.5.91:3306)/user_mon"
	conn, err := driver.Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	wr, _ := mysql.NewMysqlConnWrapper(conn)
	_, err = wr.Exec("SET @master_binlog_checksum='NONE'", nil)
	if err != nil {
		t.Fatal(err)
	}

	err = wr.WriteRegisterSlaveCommand(123, "myhost", "root", "abcd1234", 3306)
	if err != nil {
		t.Fatal(err)
	}
	_, err = wr.ReadOK()
	if err != nil {
		t.Fatal(err)
	}
	err = wr.WriteBinlogDumpCommand(123, "mysql-bin.000004", 4)
	// err = wr.WriteBinlogDumpCommand(123, "mysql-bin.000004", 772972259)
	if err != nil {
		t.Fatal(err)
	}

	dec := new(EventDecoder)
	for {
		packet, err := wr.ReadPacket()
		if err != nil {
			t.Fatal(errors.Trace(err))
		}

		ev, err := dec.Decode(packet)
		if err != nil {
			t.Fatal(err)
		}

		if ev.Header().Type == queryEvent {
			ev.Print(os.Stdout)
		}
	}
}

func TestMysqlVersion(t *testing.T) {
	old := parseMysqlVersion("5.6.1-log")
	new := parseMysqlVersion("5.6.35-log")
	t.Log(new.greaterOrEqual(old))
}
