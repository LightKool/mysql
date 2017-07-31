package binlog

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/LightKool/mysql-go"
)

type TableMapEvent struct {
	*baseEvent
	TableID           uint64
	Flags             uint16
	Database          []byte
	Table             []byte
	ColumnCount       uint64
	ColumnTypes       []byte
	ColumnMeta        []uint16
	ColumnNullability []byte
}

func (e *TableMapEvent) Decode(packet *mysql.Packet) error {
	data := append(packet.Read(6), 0, 0)
	e.TableID = binary.LittleEndian.Uint64(data)
	e.Flags = packet.ReadUint16()

	databaseLen := packet.ReadByte()
	e.Database = packet.Read(int(databaseLen))
	packet.Advance(1)

	tableLen := packet.ReadByte()
	e.Table = packet.Read(int(tableLen))
	packet.Advance(1)

	e.ColumnCount = packet.ReadPackedInteger()
	e.ColumnTypes = packet.Read(int(e.ColumnCount))

	columnMeta, err := packet.ReadTableColumnMeta(e.ColumnTypes)
	if err != nil {
		return err
	}
	e.ColumnMeta = columnMeta
	e.ColumnNullability = packet.ReadRemaining()
	if len(e.ColumnNullability) != int(e.ColumnCount+7)/8 {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (e *TableMapEvent) Print(w io.Writer) {
	e.PrintHeader(w)
	fmt.Fprintf(w, "TableID: %d\n", e.TableID)
	fmt.Fprintf(w, "Flags: %d\n", e.Flags)
	fmt.Fprintf(w, "Database: %s\n", e.Database)
	fmt.Fprintf(w, "Table: %s\n", e.Table)
	fmt.Fprintf(w, "Column count: %d\n", e.ColumnCount)
	fmt.Fprintf(w, "Column types: \n%v\n", e.ColumnTypes)
	fmt.Fprintf(w, "Column meta: \n%v\n", e.ColumnMeta)
	fmt.Fprintf(w, "Column nullability: \n%v\n", e.ColumnNullability)
	fmt.Fprintln(w)
}

type RowsQueryEvent struct {
	*baseEvent
	Query []byte
}

func (e *RowsQueryEvent) Decode(packet *mysql.Packet) error {
	packet.Advance(1)
	e.Query = packet.ReadRemaining()
	return nil
}

func (e *RowsQueryEvent) Print(w io.Writer) {
	e.PrintHeader(w)
	fmt.Fprintf(w, "Query: %s\n", e.Query)
	fmt.Fprintln(w)
}
