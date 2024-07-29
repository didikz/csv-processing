package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"time"
)

type CityDistribution struct {
	City          string
	CustomerCount int
}

func main() {
	start := time.Now() // start timing for processing time

	// open the file to a reader interface
	c, err := os.Open("../data/customers-1000000.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	// load file reader into csv reader
	// Need to set FieldsPerRecord to -1 to skip fields checking
	r := csv.NewReader(c)
	r.FieldsPerRecord = -1
	r.ReuseRecord = true
	records, err := r.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	// create hashmap to populate city with total customers based on the csv data rows
	// hashmap will looks like be ["city name": 100, ...]
	m := map[string]int{}
	for i, record := range records {
		// skip header row
		if i == 0 {
			continue
		}
		if _, found := m[record[6]]; found {
			m[record[6]]++
		} else {
			m[record[6]] = 1
		}
	}

	// convert to slice first for sorting purposes
	dc := []CityDistribution{}
	for k, v := range m {
		dc = append(dc, CityDistribution{City: k, CustomerCount: v})
	}

	// use bubble sort
	dcCount := len(dc)
	for i := 0; i < dcCount; i++ {
		swapped := false
		for j := 0; j < dcCount-i-1; j++ {
			if dc[j].CustomerCount < dc[j+1].CustomerCount {
				temp := dc[j]
				dc[j] = dc[j+1]
				dc[j+1] = temp
				swapped = true
			}
		}
		if !swapped {
			break
		}
	}

	duration := time.Since(start) // calculate processing time
	fmt.Println("rows count: ", len(records))
	fmt.Println("sorted from most customers in the city: ", dc)
	fmt.Println("processing time (ms): ", duration.Milliseconds())
}
