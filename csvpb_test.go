// Copyright 2023 The CSVPB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0

package csvpb

import (
	"bytes"
	"context"
	"encoding/csv"
	"reflect"
	"testing"
)

func TestColumns(t *testing.T) {
	t.Parallel()

	t.Run("add value", func(t *testing.T) {
		t.Parallel()

		for _, tcase := range []struct {
			name string
			data []byte
			want map[string]*column
		}{
			{
				name: "empty",
				data: []byte(`{}`),
				want: map[string]*column{},
			},
			{
				name: "single",
				data: []byte(`{"foo": "bar"}`),
				want: map[string]*column{
					"foo": {
						header: "foo",
						order:  0,
						data:   []string{"bar"},
					},
				},
			},
			{
				name: "multiple",
				data: []byte(`{"foo": "bar", "baz": "qux"}`),
				want: map[string]*column{
					"foo": {
						header: "foo",
						order:  0,
						data:   []string{"bar"},
					},
					"baz": {
						header: "baz",
						order:  1,
						data:   []string{"qux"},
					},
				},
			},
			{
				name: "nested",
				data: []byte(`{"foo": {"bar": "baz"}}`),
				want: map[string]*column{
					"foo.bar": {
						header: "foo.bar",
						order:  0,
						data:   []string{"baz"},
					},
				},
			},
			{
				name: "nested multiple",
				data: []byte(`{"foo": {"bar": "baz", "qux": "quux"}}`),
				want: map[string]*column{
					"foo.bar": {
						header: "foo.bar",
						order:  0,
						data:   []string{"baz"},
					},
					"foo.qux": {
						header: "foo.qux",
						order:  1,
						data:   []string{"quux"},
					},
				},
			},
			{
				name: "many nested",
				data: []byte(`{"foo": {"bar": "baz", "qux": "quux"}, "quux": {"quuz": "corge"}}`),
				want: map[string]*column{
					"foo.bar": {
						header: "foo.bar",
						order:  0,
						data:   []string{"baz"},
					},
					"foo.qux": {
						header: "foo.qux",
						order:  1,
						data:   []string{"quux"},
					},
					"quux.quuz": {
						header: "quux.quuz",
						order:  2,
						data:   []string{"corge"},
					},
				},
			},
			{
				name: "array of nested objects",
				data: []byte(`[{"foo": {"bar": "baz", "qux": "quux"}}, {"foo": {"bar": "corge", "qux": "grault"}}]`),
				want: map[string]*column{
					"foo.bar": {
						header: "foo.bar",
						order:  0,
						data:   []string{"baz", "corge"},
					},
					"foo.qux": {
						header: "foo.qux",
						order:  1,
						data:   []string{"quux", "grault"},
					},
				},
			},
			{
				name: "array of nested objects with different keys",
				data: []byte(`[{"foo": {"bar": "baz", "qux": "quux"}}, {"foo": {"bar": "corge", "quuz": "grault"}}]`),
				want: map[string]*column{
					"foo.bar": {
						header: "foo.bar",
						order:  0,
						data:   []string{"baz", "corge"},
					},
					"foo.qux": {
						header: "foo.qux",
						order:  1,
						data:   []string{"quux", ""},
					},
					"foo.quuz": {
						header: "foo.quuz",
						order:  2,
						data:   []string{"", "grault"},
					},
				},
			},
			{
				name: "object with array values of objects",
				data: []byte(`{"foo": [{"bar": "baz"}, {"bar": "qux"}], "quux": "quuz", "corge": "grault"}`),
				want: map[string]*column{
					"foo.bar": {
						header: "foo.bar",
						order:  0,
						data:   []string{"baz", "qux"},
					},
					"quux": {
						header: "quux",
						order:  1,
						data:   []string{"quuz", ""},
					},
					"corge": {
						header: "corge",
						order:  2,
						data:   []string{"grault", ""},
					},
				},
			},
			{
				name: "object with subobject",
				data: []byte(`{"id": 1, "name": "test", "age": {"foo": "bar"}}`),
				want: map[string]*column{
					"id": {
						header: "id",
						order:  0,
						data:   []string{"1.000000"},
					},
					"name": {
						header: "name",
						order:  1,
						data:   []string{"test"},
					},
					"age.foo": {
						header: "age.foo",
						order:  2,
						data:   []string{"bar"},
					},
				},
			},
			{
				name: "one json record with nested object",
				data: []byte(`{"id": 1, "name": "test", "age": {"foo": {"bar": "baz"}}}`),
				want: map[string]*column{
					"id": {
						header: "id",
						order:  0,
						data:   []string{"1.000000"},
					},
					"name": {
						header: "name",
						order:  1,
						data:   []string{"test"},
					},
					"age.foo.bar": {
						header: "age.foo.bar",
						order:  2,
						data:   []string{"baz"},
					},
				},
			},
		} {
			tcase := tcase

			t.Run(tcase.name, func(t *testing.T) {
				t.Parallel()

				// Convert the data to a struct.
				list, err := Decode(DecodeTypeJSON, tcase.data)
				if err != nil {
					t.Fatal(err)
				}

				cols := newColumns(withBuf(rowBufferForList(list)))

				t.Logf("buffer size: %d\n", cols.buf)

				for _, value := range list.GetValues() {
					if err := cols.addValue("", value); err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
				}

				cols.trimParents()

				for _, got := range cols.m {
					want, ok := tcase.want[got.header]
					if !ok {
						t.Logf("got: %+v for header %q with len=%d", got, got.header, len(got.data))
						t.Logf("want: %+v", want)

						t.Fatalf("unexpected column: %s", got.header)
					}

					if !reflect.DeepEqual(got.data, want.data) {
						t.Logf("got: %+v with len=%d", got, len(got.data))
						t.Logf("want: %+v", want)

						t.Fatalf("unexpected column: %s", got.header)
					}
				}
			})
		}
	})
}

func TestWrite(t *testing.T) {
	t.Parallel()

	for _, tcase := range []struct {
		name       string
		data       []byte
		decodeType DecodeType
		want       [][]string
	}{
		{
			name:       "one json record",
			data:       []byte(`{"id": 1, "name": "test", "age": 42}`),
			decodeType: DecodeTypeJSON,
			want: [][]string{
				{"id", "name", "age"},
				{"1.000000", "test", "42.000000"},
			},
		},
		{
			name:       "one json record with null",
			data:       []byte(`{"id": 1, "name": "test", "age": null}`),
			decodeType: DecodeTypeJSON,
			want: [][]string{
				{"id", "name", "age"},
				{"1.000000", "test", ""},
			},
		},
		{
			name:       "one json record with array",
			data:       []byte(`{"id": 1, "name": "test", "age": [1, 2, 3]}`),
			decodeType: DecodeTypeJSON,
			want: [][]string{
				{"id", "name", "age"},
				{"1.000000", "test", "[1.000000,2.000000,3.000000]"},
			},
		},
		{
			name:       "one json record with object",
			data:       []byte(`{"id": 1, "name": "test", "age": {"foo": "bar"}}`),
			decodeType: DecodeTypeJSON,
			want: [][]string{
				{"id", "name", "age.foo"},
				{"1.000000", "test", "bar"},
			},
		},
		{
			name:       "one json record with nested object",
			data:       []byte(`{"id": 1, "name": "test", "age": {"foo": {"bar": "baz"}}}`),
			decodeType: DecodeTypeJSON,
			want: [][]string{
				{"id", "name", "age.foo.bar"},
				{"1.000000", "test", "baz"},
			},
		},
		{
			name:       "array of objects",
			decodeType: DecodeTypeJSON,
			data:       []byte(`[{ "id": 1, "name": "test", "age": 42 }, { "id": 2, "name": "test2", "age": 43 }]`),
			want: [][]string{
				{"id", "name", "age"},
				{"1.000000", "test", "42.000000"},
				{"2.000000", "test2", "43.000000"},
			},
		},
	} {
		tcase := tcase

		t.Run(tcase.name, func(t *testing.T) {
			t.Parallel()

			// Create a buffer that will write to [][]string.
			var buf bytes.Buffer
			csvWriter := csv.NewWriter(&buf)

			// Turn data into a list.
			list, err := Decode(DecodeTypeJSON, tcase.data)
			if err != nil {
				t.Fatal(err)
			}

			// Write the data to the buffer.
			gidariWriter := NewListWriter(csvWriter)

			if err := gidariWriter.Write(context.Background(), list); err != nil {
				t.Fatal(err)
			}

			// Flush the buffer.
			csvWriter.Flush()

			// Read the buffer.
			r := csv.NewReader(&buf)
			got, err := r.ReadAll()
			if err != nil {
				t.Fatal(err)
			}

			// Make sure got and want are the same, ignoring order
			// of the headers.
			gotHeaderOrder := make(map[string]int)
			for i, header := range got[0] {
				gotHeaderOrder[header] = i
			}

			wantHeaderOrder := make(map[string]int)
			for i, header := range tcase.want[0] {
				wantHeaderOrder[header] = i
			}

			goRowsByHeader := make(map[string][]string)
			for _, row := range got[1:] {
				for header, i := range gotHeaderOrder {
					goRowsByHeader[header] = append(goRowsByHeader[header], row[i])
				}
			}

			wantRowsByHeader := make(map[string][]string)
			for _, row := range tcase.want[1:] {
				for header, i := range wantHeaderOrder {
					wantRowsByHeader[header] = append(wantRowsByHeader[header], row[i])
				}
			}

			if !reflect.DeepEqual(goRowsByHeader, wantRowsByHeader) {
				t.Logf("got: %+v", got)
				t.Logf("want: %+v", tcase.want)

				t.Fatal("unexpected rows")
			}
		})
	}
}

func TestRowBufferForList(t *testing.T) {
	t.Parallel()

	for _, tcase := range []struct {
		name       string
		data       []byte
		decodeType DecodeType
		want       int
	}{
		{
			"d0=1 d1=1",
			[]byte(`{"x": [{"y": [{"z": 1}]}]}`),
			DecodeTypeJSON,
			1,
		},
		{
			"d0=1 d1=2",
			[]byte(`{"x": [{"": [{"z": 1}, {"z": 2}]}]}`),
			DecodeTypeJSON,
			2,
		},
		{
			"d0=2 d1=1",
			[]byte(`{"x": [{"y": [{"z": 1}]}, {"y": [{"z": 2}]}]}`),
			DecodeTypeJSON,
			2,
		},
		{
			"d0=2 d1=3",
			[]byte(`{"x": [{"y": [{"z": 1}, {"z": 2}, {"z": 3}]}, {"y": [{"z": 4}, {"z": 5}, {"z": 6}]}]}`),
			DecodeTypeJSON,
			6,
		},
		{
			"d0=0 with many objects",
			[]byte(`{"x": {"y": "x"}, "b": {"z": "a"}}`),
			DecodeTypeJSON,
			1,
		},
		{
			"d0=1",
			[]byte(`[{"x": {"y": 1}}, {"x": {"z": 1}}]`),
			DecodeTypeJSON,
			2,
		},
	} {
		tcase := tcase

		t.Run(tcase.name, func(t *testing.T) {
			t.Parallel()

			// Turn data into a list.
			list, err := Decode(DecodeTypeJSON, tcase.data)
			if err != nil {
				t.Fatal(err)
			}

			got := rowBufferForList(list)

			if got != tcase.want {
				t.Fatalf("got %d, want %d", got, tcase.want)
			}
		})
	}
}

func BenchmarkListWriter(b *testing.B) {
	// Create a buffer that will write to [][]string.
	var buf bytes.Buffer
	csvWriter := csv.NewWriter(&buf)

	// Turn data into a list.
	list, err := Decode(DecodeTypeJSON, []byte(`{
"foo": "bar",
"baz": 42,
"qux": [1, 2, 3],
"quux": {
	"corge": "grault",
	"garply": "waldo",
	"fred": "plugh"
},
"xyzzy": null
}`))
	if err != nil {
		b.Fatal(err)
	}

	// Write the data to the buffer.
	gidariWriter := NewListWriter(csvWriter)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := gidariWriter.Write(context.Background(), list); err != nil {
			b.Fatal(err)
		}
	}
}
