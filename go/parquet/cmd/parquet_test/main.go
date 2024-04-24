// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"log"
	"os"

	//"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/cmd/parquet_test/buffer"
	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/metadata"
	"github.com/apache/arrow/go/v17/parquet/schema"
)

func usage() {
	fmt.Fprintf(os.Stderr, "usage: %s [inputfile]\n", os.Args[0])
	os.Exit(2)
}

func main() {
	if len(os.Args) == 1 {
		usage()
	}
	fn := os.Args[1]
	f, err := os.Open(fn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open file: %v", err)
		os.Exit(1)
	}
	buf, err := buffer.New(f)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create buffer: %v", err)
		os.Exit(1)
	}
	// NOTE: Can use WithReadProps for cache size and streaming.
	// Also look at WithMetadata. If data is too large, may want to read from file
	// from OpenParquetFile instead.
	prd, err := file.NewParquetReader(buf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "new parquest reader: %v", err)
		os.Exit(1)
	}
	prdMetadata := prd.MetaData()
	fmt.Println("Version:", prdMetadata.Version())
	fmt.Println("Created By:", prdMetadata.GetCreatedBy())
	fmt.Println("Num Rows:", prd.NumRows())

	keyvaluemeta := prdMetadata.KeyValueMetadata()
	if keyvaluemeta != nil {
		fmt.Println("Key Value Metadata:", keyvaluemeta.Len(), "entries")
		keys := keyvaluemeta.Keys()
		values := keyvaluemeta.Values()
		for i := 0; i < keyvaluemeta.Len(); i++ {
			fmt.Printf(">> Key nr %d:\n%q: %q\n", i, keys[i], values[i])
		}
	}

	fmt.Println("Number of RowGroups:", prd.NumRowGroups())
	fmt.Println("Number of Real Columns:", prdMetadata.Schema.Root().NumFields())
	fmt.Println("Number of Columns:", prdMetadata.Schema.NumColumns())
	selectedColumns := []int{}
	for i := 0; i < prdMetadata.Schema.NumColumns(); i++ {
		selectedColumns = append(selectedColumns, i)
	}
	for _, c := range selectedColumns {
		descr := prdMetadata.Schema.Column(c)
		fmt.Printf("Column %d: %s (%s", c, descr.Path(), descr.PhysicalType())
		if descr.ConvertedType() != schema.ConvertedTypes.None {
			fmt.Printf("/%s", descr.ConvertedType())
			if descr.ConvertedType() == schema.ConvertedTypes.Decimal {
				dec := descr.LogicalType().(*schema.DecimalLogicalType)
				fmt.Printf("(%d,%d)", dec.Precision(), dec.Scale())
			}
		}
		fmt.Print(")\n")
	}
	return
	for r := 0; r < prd.NumRowGroups(); r++ {
		fmt.Println("--- Row Group:", r, " ---")

		rgr := prd.RowGroup(r)
		rowGroupMeta := rgr.MetaData()
		fmt.Println("--- Total Bytes:", rowGroupMeta.TotalByteSize(), " ---")
		fmt.Println("--- Rows:", rgr.NumRows(), " ---")

		for _, c := range selectedColumns {
			chunkMeta, err := rowGroupMeta.ColumnChunk(c)
			if err != nil {
				log.Fatal(err)
			}

			fmt.Println("Column", c)
			if set, _ := chunkMeta.StatsSet(); set {
				stats, err := chunkMeta.Statistics()
				if err != nil {
					log.Fatal(err)
				}
				fmt.Printf(" Values: %d", chunkMeta.NumValues())
				if stats.HasMinMax() {
					fmt.Printf(", Min: %v, Max: %v",
						metadata.GetStatValue(stats.Type(), stats.EncodeMin()),
						metadata.GetStatValue(stats.Type(), stats.EncodeMax()))
				}
				if stats.HasNullCount() {
					fmt.Printf(", Null Values: %d", stats.NullCount())
				}
				if stats.HasDistinctCount() {
					fmt.Printf(", Distinct Values: %d", stats.DistinctCount())
				}
				fmt.Println()
			} else {
				fmt.Println(" Values:", chunkMeta.NumValues(), "Statistics Not Set")
			}

			fmt.Print(" Compression: ", chunkMeta.Compression())
			fmt.Print(", Encodings:")
			for _, enc := range chunkMeta.Encodings() {
				fmt.Print(" ", enc)
			}
			fmt.Println()
			fmt.Print(" Uncompressed Size: ", chunkMeta.TotalUncompressedSize())
			fmt.Println(", Compressed Size:", chunkMeta.TotalCompressedSize())
		}
	}

	fmt.Println("--- Values ---")

}
