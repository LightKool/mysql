package binlog

import (
	"github.com/LightKool/mysql-go"
)

type binlogPacket struct {
	*mysql.Packet
}

func newBinlogPacket(packet *mysql.Packet) *binlogPacket {
	return &binlogPacket{packet}
}
