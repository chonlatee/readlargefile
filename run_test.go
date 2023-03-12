package main

import (
	"fmt"
	"testing"
)

func BenchmarkConcurrent(b *testing.B) {
	tableBenchmarks := []struct {
		name    string
		file    string
		inputs  [][]int
		benchFn func(file string, numWorkers, batchSize int) result
	}{
		{
			name:   "Sequential",
			file:   "./data/sample.txt",
			inputs: [][]int{{0, 0}},
			benchFn: func(file string, numWorkers, batchSize int) result {
				return Sequential(file)
			},
		},
		{
			name:   "Concurrent",
			file:   "./data/sample.txt",
			inputs: [][]int{{1, 1}, {1, 1000}, {10, 1000}, {10, 10000}, {10, 100000}},
			benchFn: func(file string, numWorkers, batchSize int) result {
				return Concurrent(file, numWorkers, batchSize)
			},
		},
	}

	for _, tb := range tableBenchmarks {
		for _, x := range tb.inputs {
			numWorker := x[0]
			batchSize := x[1]

			bName := fmt.Sprintf("%s %03d workers %04d batchSize", tb.name, numWorker, batchSize)
			b.Run(bName, func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					tb.benchFn(tb.file, numWorker, batchSize)
				}
			})
		}
	}
}
