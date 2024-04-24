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
	"os"

	//"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/cmd/parquet_test/buffer"
	"github.com/apache/arrow/go/v17/parquet/file"
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
	prd, err := file.NewParquetReader(buf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "new parquest reader: %v", err)
		os.Exit(1)
	}
	prdMetadata := prd.MetaData()
	fmt.Println("Version:", prdMetadata.Version())
	fmt.Println("Created By:", prdMetadata.GetCreatedBy())
	fmt.Println("Num Rows:", prd.NumRows())
}
