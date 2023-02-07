// Copyright 2022 The CSVPB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
package csvpb

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"google.golang.org/protobuf/types/known/structpb"
)

// ErrUnsupportedValueType is returned when a value type is not supported.
var ErrUnsupportedValueType = fmt.Errorf("unsupported value type")

type column struct {
	header string
	order  int
	data   []string
}

type columns struct {
	m           map[string]*column
	alphabetize bool
	buf         int
	currentPos  int
}

type columnsOpt func(*columns)

func newColumns(opts ...columnsOpt) *columns {
	cols := &columns{m: make(map[string]*column)}

	for _, opt := range opts {
		opt(cols)
	}

	return cols
}

func withAlphabetize(alphabetize bool) columnsOpt {
	return func(cols *columns) {
		cols.alphabetize = alphabetize
	}
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

func (cols *columns) addColumn(header string) {
	if _, ok := cols.m[header]; ok {
		return
	}

	cols.m[header] = &column{
		header: header,
		order:  len(cols.m),
		data:   make([]string, cols.buf),
	}

	if cols.alphabetize {
		cols.reorderAlphabetically()
	}
}

func (cols *columns) addData(header string, data string) {
	// If the column doesn't exist, then we need to create it.
	if _, ok := cols.m[header]; !ok {
		cols.addColumn(header)
	}

	cols.m[header].data[cols.currentPos] = data
}

func (cols *columns) addStruct(key string, obj *structpb.Struct) error {
	// If the key is not empty, then that means that we are in a nested
	// object. To deal with this case, we create a new columns map buffered
	// with only one row and process the values for that map and object.
	focus := cols
	if key != "" {
		focus = newColumns(withBuf(1), withAlphabetize(cols.alphabetize))
	}

	for fieldName, fieldValue := range obj.GetFields() {
		err := focus.addValue(fieldName, fieldValue)
		if err != nil {
			return fmt.Errorf("failed to add struct value: %w", err)
		}
	}

	if focus != cols {
		for _, subColumn := range focus.m {
			newFieldName := fmt.Sprintf("%s.%s", key, subColumn.header)
			cols.addData(newFieldName, subColumn.data[0])
		}
	}

	focus.currentPos++

	return nil
}

func (cols *columns) addList(key string, list *structpb.ListValue) {
	var buf strings.Builder

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
		}

		if i != len(list.GetValues())-1 {
			buf.WriteString(",")
		}
	}

	buf.WriteString("]")

	// Join the values with a comma.
	cols.addData(key, buf.String())
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
		cols.addList(key, valType.ListValue)
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

// Write writes the ListValue to CSV.
func (w *ListWriter) Write(ctx context.Context, list *structpb.ListValue) error {
	// columns is a map of column headers to the column data.
	columns := newColumns(
		withBuf(len(list.GetValues())),
		withAlphabetize(w.alphabetizeHeaders))

	for _, value := range list.Values {
		err := columns.addValue("", value)
		if err != nil {
			return fmt.Errorf("failed to add value: %w", err)
		}
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

	for i := 0; i < columns.currentPos; i++ {
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
