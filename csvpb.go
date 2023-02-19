// Copyright 2023 The CSVPB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0

// csvpb is a package for writing CSV files from a structpb.ListValue.
package csvpb

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"

	"google.golang.org/protobuf/types/known/structpb"
)

// ErrUnsupportedValueType is returned when a value type is not supported.
var ErrUnsupportedValueType = fmt.Errorf("unsupported value type")

type column struct {
	parent *column
	header string
	order  int
	data   []string
	rowNum int
}

// root is the root of the column tree, the oldest ancestor.
func (col *column) root() *column {
	if col.parent == nil {
		return col
	}

	return col.parent.root()
}

func (col *column) currentRowNum() int {
	return col.root().rowNum
}

func (col *column) updateRowNum() {
	col.root().rowNum++
}

type columns struct {
	m             map[string]*column
	buf           int
	currentColNum int
}

type columnsOpt func(*columns)

func newColumns(opts ...columnsOpt) *columns {
	cols := &columns{m: make(map[string]*column)}

	for _, opt := range opts {
		opt(cols)
	}

	return cols
}

func withBuf(buf int) columnsOpt {
	return func(cols *columns) {
		cols.buf = buf
	}
}

func (cols *columns) reorderAlphabetically() {
	columns := make([]*column, len(cols.m))
	for _, column := range cols.m {
		columns[column.order] = column
	}

	// sort the columns alphabetically
	sort.Slice(columns, func(i, j int) bool {
		return columns[i].header < columns[j].header
	})

	// update the order
	for i, column := range columns {
		column.order = i
	}

	// update the map
	cols.m = make(map[string]*column)
	for _, column := range columns {
		cols.m[column.header] = column
	}
}

func (cols *columns) addChildColumn(parent *column, header string) {
	if _, ok := cols.m[header]; ok {
		return
	}

	cols.m[header] = &column{
		parent: parent,
		header: header,
	}
}

func (cols *columns) addColumn(header string) {
	cols.addChildColumn(nil, header)
}

func (cols *columns) addChildData(parent *column, key string, data string) {
	// If the column doesn't exist, then we need to create it.
	if _, ok := cols.m[key]; !ok {
		cols.addChildColumn(parent, key)
	}

	// If the data is empty update it to be the size of the buffer.
	if len(cols.m[key].data) == 0 {
		cols.m[key].data = make([]string, cols.buf)
		cols.m[key].order = cols.currentColNum
		cols.currentColNum++
	}

	col := cols.m[key]

	col.data[col.currentRowNum()] = data
}

func (cols *columns) addData(key string, data string) {
	cols.addChildData(nil, key, data)
}

// trimParents will trim the parent data from the columns.
func (cols *columns) trimParents() {
	for _, column := range cols.m {
		if len(column.data) == 0 {
			delete(cols.m, column.header)
		}
	}
}

func (cols *columns) addStruct(key string, obj *structpb.Struct) error {
	cols.addColumn(key)

	// Add the parent column to the columns.
	focus := cols
	if key != "" {
		// If the key is not empty, then that means that we are in a
		// nested object. To deal with this case, we create a new object
		// and add it to the columns.
		focus = newColumns(withBuf(rowBufferForStruct(obj)))
	}

	for fieldName, fieldValue := range obj.GetFields() {
		err := focus.addChildValue(focus.m[key], fieldName, fieldValue)
		if err != nil {
			return fmt.Errorf("failed to add struct value: %w", err)
		}
	}

	if focus != cols {
		for _, subColumn := range focus.m {
			// If the subColumn has no data, then do nothing.
			if len(subColumn.data) == 0 {
				continue
			}

			newFieldName := fmt.Sprintf("%s.%s", key, subColumn.header)

			parent := cols.m[key]
			cols.addChildData(parent, newFieldName, subColumn.data[0])
		}
	}

	// If there is no column, there is nothing to update.
	if cols.m[key] != nil {
		cols.m[key].updateRowNum()
	}

	return nil
}

//nolint:cyclop
func (cols *columns) addList(key string, list *structpb.ListValue) error {
	var buf strings.Builder

	const minBufLen = 3

	buf.WriteString("[")

	for i, value := range list.GetValues() {
		// Stringify the value.
		switch valType := value.Kind.(type) {
		case *structpb.Value_StringValue:
			buf.WriteString(valType.StringValue)
		case *structpb.Value_NumberValue:
			buf.WriteString(fmt.Sprintf("%f", valType.NumberValue))
		case *structpb.Value_BoolValue:
			buf.WriteString(fmt.Sprintf("%t", valType.BoolValue))
		case *structpb.Value_NullValue:
			buf.WriteString("")
		case *structpb.Value_StructValue:
			err := cols.addStruct(key, valType.StructValue)
			if err != nil {
				return fmt.Errorf("failed to add list value: %w", err)
			}

			// In the struct case, we need to exclude the key
			// from being added to the list.
			continue
		default:
			return fmt.Errorf("%w: %T", ErrUnsupportedValueType, valType)
		}

		if i != len(list.GetValues())-1 {
			buf.WriteString(",")
		}
	}

	buf.WriteString("]")

	// If the buffer is greater than two (i.e. []), then we need to add
	// the data to the column.
	if buf.Len() >= minBufLen {
		cols.addData(key, buf.String())
	}

	return nil
}

func (cols *columns) addChildValue(parent *column, key string, value *structpb.Value) error {
	switch valType := value.Kind.(type) {
	case *structpb.Value_NullValue:
		cols.addChildData(parent, key, "")
	case *structpb.Value_NumberValue:
		cols.addChildData(parent, key, fmt.Sprintf("%f", valType.NumberValue))
	case *structpb.Value_StringValue:
		cols.addChildData(parent, key, valType.StringValue)
	case *structpb.Value_BoolValue:
		cols.addChildData(parent, key, fmt.Sprintf("%t", valType.BoolValue))
	case *structpb.Value_StructValue:
		return cols.addStruct(key, valType.StructValue)
	case *structpb.Value_ListValue:
		return cols.addList(key, valType.ListValue)
	default:
		return fmt.Errorf("%w: %T", ErrUnsupportedValueType, valType)
	}

	return nil
}

func (cols *columns) addValue(key string, value *structpb.Value) error {
	switch valType := value.Kind.(type) {
	case *structpb.Value_NullValue:
		cols.addData(key, "")
	case *structpb.Value_NumberValue:
		cols.addData(key, fmt.Sprintf("%f", valType.NumberValue))
	case *structpb.Value_StringValue:
		cols.addData(key, valType.StringValue)
	case *structpb.Value_BoolValue:
		cols.addData(key, fmt.Sprintf("%t", valType.BoolValue))
	case *structpb.Value_StructValue:
		return cols.addStruct(key, valType.StructValue)
	case *structpb.Value_ListValue:
		return cols.addList(key, valType.ListValue)
	default:
		return fmt.Errorf("%w: %T", ErrUnsupportedValueType, valType)
	}

	return nil
}

// Writer is a CSV writer.
type Writer interface {
	Write(record []string) error
}

// ListWriter is used to write a structpb.ListValue to CSV, using a CSV writer.
type ListWriter struct {
	alphabetizeHeaders bool
	writer             Writer
}

// ListWriterOption is used to configure the ListWriter.
type ListWriterOption func(*ListWriter)

// NewListWriter creates a new ListWriter for writing a structpb.ListValue to
// CSV.
func NewListWriter(writer Writer, opts ...ListWriterOption) *ListWriter {
	listWriter := &ListWriter{
		writer: writer,
	}

	for _, opt := range opts {
		opt(listWriter)
	}

	return listWriter
}

// WithAlphabetizeHeaders configures the ListWriter to alphabetize the headers
// when writing the CSV.
func WithAlphabetizeHeaders() ListWriterOption {
	return func(listWriter *ListWriter) {
		listWriter.alphabetizeHeaders = true
	}
}

// rowBufferForStruct will recursively iterate over all fields and count the number
// of columns in every nested struct.
func rowBufferForStruct(obj *structpb.Struct) int {
	var buf int

	for _, value := range obj.GetFields() {
		valType := value.Kind
		if _, ok := valType.(*structpb.Value_ListValue); !ok {
			continue
		}

		list, ok := valType.(*structpb.Value_ListValue)
		if !ok {
			continue
		}

		buf += rowBufferForList(list.ListValue)
	}

	return int(math.Max(float64(buf), 1))
}

// rowBufferForList will return the number of rows that should be creatd for the given
// structpb.Listvalue.
func rowBufferForList(list *structpb.ListValue) int {
	var buf int

	// Recursive call this function to get the number of unique columns
	// across ALL objects in the list.
	for _, value := range list.GetValues() {
		valType := value.Kind
		if _, ok := valType.(*structpb.Value_StructValue); !ok {
			continue
		}

		obj, ok := valType.(*structpb.Value_StructValue)
		if !ok {
			continue
		}

		buf += rowBufferForStruct(obj.StructValue)
	}

	return buf
}

// Write writes the ListValue to CSV.
func (w *ListWriter) Write(ctx context.Context, list *structpb.ListValue) error {
	rowCount := rowBufferForList(list)

	// columns is a map of column headers to the column data.
	columns := newColumns(withBuf(rowCount))

	for _, value := range list.Values {
		err := columns.addValue("", value)
		if err != nil {
			return fmt.Errorf("failed to add value: %w", err)
		}
	}

	// Remove all nodes that do not contain data to write. These include
	// parent rows for data organization.
	columns.trimParents()

	// Reorder the columns to be in alphabetical order.
	if w.alphabetizeHeaders {
		columns.reorderAlphabetically()
	}

	// Put the data in form of a slice of slices, where the first slice is
	// the headers and the rest are the data.
	data := make([][]string, len(list.Values)+1)
	data[0] = make([]string, len(columns.m))

	for _, column := range columns.m {
		data[0][column.order] = column.header
	}

	// Write the header data.
	err := w.writer.Write(data[0])
	if err != nil {
		return fmt.Errorf("failed to write csv header: %w", err)
	}

	for i := 0; i < rowCount; i++ {
		row := make([]string, len(columns.m))

		for _, column := range columns.m {
			column := column

			row[column.order] = column.data[i]
		}

		err := w.writer.Write(row)
		if err != nil {
			return fmt.Errorf("failed to write csv data: %w", err)
		}
	}

	return nil
}
