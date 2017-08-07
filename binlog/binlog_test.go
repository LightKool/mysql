package binlog

import (
	"os"
	"testing"

	"github.com/LightKool/mysql-go"
	"github.com/juju/errors"
)

func TestDriver(t *testing.T) {
	driver := &mysql.MySQLDriver{}
	dsn := "root:abcd1234@tcp(10.17.5.91:3306)/user_mon"
	wr, _ := mysql.NewConnWrapper(driver)
	err := wr.Connect(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer wr.Close()

	_, err = wr.Exec("SET @master_binlog_checksum='NONE'", nil)
	if err != nil {
		t.Fatal(err)
	}

	err = wr.WriteRegisterSlaveCommand(123, "myhost", "root", "abcd1234", 3306)
	if err != nil {
		t.Fatal(err)
	}
	err = wr.ReadOK()
	if err != nil {
		t.Fatal(err)
	}
	// err = wr.WriteBinlogDumpCommand(123, "mysql-bin.000004", 4)
	err = wr.WriteBinlogDumpCommand(123, "mysql-bin.000004", 772972259)
	if err != nil {
		t.Fatal(err)
	}

	dec := new(EventDecoder)
	dec.tables = make(map[uint64]*TableMapEvent)
	for {
		packet, err := wr.ReadPacket()
		if err != nil {
			t.Fatal(errors.Trace(err))
		}

		ev, err := dec.Decode(packet)
		if err != nil {
			t.Fatal(err)
		}

		// if e, ok := ev.(*WriteRowsEvent); ok {
		// 	if string(e.Table.TableName) == "TB_USER" {
		ev.Print(os.Stdout)
		// 		break
		// 	}
		// }
	}
}

func TestMysqlVersion(t *testing.T) {
	old := parseMysqlVersion("5.6.1-log")
	new := parseMysqlVersion("5.6.35-log")
	t.Log(new.greaterOrEqual(old))
}

func TestBitSet(t *testing.T) {
	bitmap := []byte{252, 7}
	t.Log(bitmap[1>>3]&(1<<(1&7)) > 0)
}
