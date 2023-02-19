// Copyright 2023 The CSVPB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0

package csvpb_test

import (
	"context"
	"encoding/csv"
	"log"
	"os"

	"github.com/alpstable/csvpb"
)

func ExampleListWriter_Write() {
	// Create a writer object using the "encoding/csv" package.
	writer := csv.NewWriter(os.Stdout)

	// Create a new list writer.
	listWriter := csvpb.NewListWriter(writer, csvpb.WithAlphabetizeHeaders())

	// Create a structpb.List to write as a CSV to stdout.
	exJSON := []byte(`{"id": 1, "name": "test", "age": null}`)

	exList, err := csvpb.Decode(csvpb.DecodeTypeJSON, exJSON)
	if err != nil {
		log.Fatalf("failed to decode JSON: %v", err)
	}

	// Write a list to the list writer.
	if err := listWriter.Write(context.TODO(), exList); err != nil {
		log.Fatalf("failed to write list: %v", err)
	}

	// Flush the writer.
	writer.Flush()

	// Output:
	// age,id,name
	// ,1.000000,test
}
