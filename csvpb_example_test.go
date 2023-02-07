package gcsv_test

import (
	"context"
	"encoding/csv"
	"log"
	"os"

	"github.com/alpstable/gcsv"
)

func ExampleListWriter_Write() {
	// Create a writer object using the "encoding/csv" package.
	writer := csv.NewWriter(os.Stdout)

	// Create a new list writer.
	listWriter := gcsv.NewListWriter(writer, gcsv.WithAlphabetizeHeaders())

	// Create a structpb.List to write as a CSV to stdout.
	exJSON := []byte(`{"id": 1, "name": "test", "age": null}`)

	exList, err := gcsv.Decode(gcsv.DecodeTypeJSON, exJSON)
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
