package main

import (
	"bytes"
	"io"
	"log"
	"os"
	"sort"
)

type Record struct {
	Key   [10]byte
	Value [90]byte
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 3 {
		log.Fatalf("Usage: %v <input_file> <output_file>\n", os.Args[0])
	}

	log.Printf("Starting the sorting process for file: %s\n", os.Args[1])

	inputFileName := os.Args[1]

	inputFile, err := os.Open(inputFileName)
	if err != nil {
		log.Fatalf("Error opening input file: %v", err)
	}
	defer inputFile.Close()

	var records []Record
	for {
		var key [10]byte
		var value [90]byte

		_, err := inputFile.Read(key[:])
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Println("Error reading key:", err)
		}

		_, err = inputFile.Read(value[:])
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Println("Error reading value:", err)
		}

		rec := Record{key, value}
		records = append(records, rec)
	}

	sort.Slice(records, func(i, j int) bool {
		return bytes.Compare(records[i].Key[:], records[j].Key[:]) <= 0
	})

	outputFileName := os.Args[2]
	outputFile, err := os.Create(outputFileName)
	if err != nil {
		log.Fatalf("Error creating output file: %v", err)
	}
	defer outputFile.Close()

	for _, rec := range records {
		if _, err := outputFile.Write(rec.Key[:]); err != nil {
			log.Println("Error writing key:", err)
		}
		if _, err := outputFile.Write(rec.Value[:]); err != nil {
			log.Println("Error writing value:", err)
		}
	}

	log.Println("Sorting process completed successfully!")
}
