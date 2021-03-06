package binlog

import (
	"os"
	"testing"

	"github.com/LightKool/mysql-go"
)

func TestDriver(t *testing.T) {
	dsn := "root:abcd1234@tcp(10.17.5.91:3306)/user_mon"
	wr := mysql.NewConnWrapper()
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
	err = wr.WriteBinlogDumpCommand(123, "mysql-bin.000005", 28617898)
	if err != nil {
		t.Fatal(err)
	}

	dec := new(EventDecoder)
	dec.tables = make(map[uint64]*TableMapEvent)
	for {
		packet, err := wr.ReadPacket()
		if err != nil {
			t.Fatal(err)
		}

		ev, err := dec.decode(packet)
		if err != nil {
			t.Fatal(err)
		}

		ev.Print(os.Stdout)
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
