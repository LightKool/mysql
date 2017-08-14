package binlog

import (
	"encoding/binary"
	"fmt"
	"io"
)

type TableMapEvent struct {
	*baseEvent
	TableID           uint64
	Flags             uint16
	Database          []byte
	TableName         []byte
	ColumnCount       uint64
	ColumnTypes       []byte
	ColumnMeta        []uint16
	ColumnNullability []byte
}

func (e *TableMapEvent) Decode(dec *EventDecoder) error {
	packet := e.header.packet
	data := append(packet.Read(6), 0, 0)
	e.TableID = binary.LittleEndian.Uint64(data)
	e.Flags = packet.readUint16()

	databaseLen := packet.readByte()
	e.Database = packet.Read(int(databaseLen))
	packet.Skip(1)

	tableLen := packet.readByte()
	e.TableName = packet.Read(int(tableLen))
	packet.Skip(1)

	e.ColumnCount = packet.ReadPackedInteger()
	e.ColumnTypes = packet.Read(int(e.ColumnCount))

	columnMeta, err := packet.readTableColumnMeta(e.ColumnTypes)
	if err != nil {
		return err
	}
	e.ColumnMeta = columnMeta
	e.ColumnNullability = packet.Read(-1)
	if len(e.ColumnNullability) != int(e.ColumnCount+7)>>3 {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (e *TableMapEvent) postDecode(dec *EventDecoder) error {
	dec.tables[e.TableID] = e
	return nil
}

func (e *TableMapEvent) Print(w io.Writer) {
	e.printHeader(w)
	fmt.Fprintf(w, "TableID: %d\n", e.TableID)
	fmt.Fprintf(w, "Flags: %d\n", e.Flags)
	fmt.Fprintf(w, "Database: %s\n", e.Database)
	fmt.Fprintf(w, "Table: %s\n", e.TableName)
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

func (e *RowsQueryEvent) Decode(dec *EventDecoder) error {
	packet := e.header.packet
	packet.Skip(1)
	e.Query = packet.Read(-1)
	return nil
}

func (e *RowsQueryEvent) Print(w io.Writer) {
	e.printHeader(w)
	fmt.Fprintf(w, "Query: %s\n", e.Query)
	fmt.Fprintln(w)
}

type RowsEvent struct {
	*baseEvent
	TableID        uint64
	Table          *TableMapEvent
	Flags          uint16
	ExtraData      []byte
	ColumnCount    uint64
	Columns        []byte
	UpdatedColumns []byte
	Rows           [][]interface{}
}

func (e *RowsEvent) Decode(dec *EventDecoder) error {
	packet := e.header.packet

	e.TableID = packet.ReadUintBySize(6)
	e.Table = dec.tables[e.TableID]
	e.Flags = packet.readUint16() // reserved

	extraDataLen := packet.readUint16()
	e.ExtraData = packet.Read(int(extraDataLen) - 2)

	e.ColumnCount = packet.ReadPackedInteger()
	e.Columns = packet.Read(int(e.ColumnCount+7) >> 3)
	if e.header.Type == UpdateRowsEventType {
		e.UpdatedColumns = packet.Read(int(e.ColumnCount+7) >> 3)
	}

	e.Rows = make([][]interface{}, 0)
	for !packet.EOF() {
		e.decodeOneRow(e.Columns)
		if e.header.Type == UpdateRowsEventType {
			e.decodeOneRow(e.UpdatedColumns)
		}
	}
	return nil
}

func (e *RowsEvent) decodeOneRow(includedColumns []byte) (err error) {
	packet := e.header.packet

	var includedColumnsCount int
	for i := 0; i < int(e.ColumnCount); i++ {
		if isBitSet(includedColumns, i) {
			includedColumnsCount++
		}
	}
	nullColumns := packet.Read((includedColumnsCount + 7) >> 3)

	row, skipped, index := make([]interface{}, includedColumnsCount), 0, 0
	for i := 0; i < int(e.ColumnCount); i++ {
		if !isBitSet(includedColumns, i) {
			skipped++
			continue
		}
		index = i - skipped
		if !isBitSet(nullColumns, index) {
			row[index], err = packet.readTableColumnValue(e.Table.ColumnTypes[i], e.Table.ColumnMeta[i])
			if err != nil {
				return
			}
		}
	}
	e.Rows = append(e.Rows, row)
	return
}

func (e *RowsEvent) Print(w io.Writer) {
	e.printHeader(w)
	fmt.Fprintf(w, "TableID: %d\n", e.TableID)
	fmt.Fprintf(w, "Table: %s.%s\n", e.Table.Database, e.Table.TableName)
	fmt.Fprintf(w, "Flags: %d\n", e.Flags)
	fmt.Fprintf(w, "Column count: %d\n", e.ColumnCount)
	fmt.Fprintf(w, "Columns: %v\n", e.Columns)
	e.printRows(w)
	fmt.Fprintln(w)
}

func (e *RowsEvent) printRows(w io.Writer) {
	fmt.Fprintln(w, "Rows:")
	for _, row := range e.Rows {
		fmt.Fprintf(w, "%v\n", row)
	}
}
