package main

import (
	"bufio"
	"context"
	"log"
	"os"
	"strings"
	"sync"
)

type processed struct {
	numRows    int
	fullNames  []string
	firstNames []string
	months     []string
}

type result struct {
	donationMonthFreq map[string]int
	commonNameCount   int
	commonName        string
	peopleCount       int
	numRows           int
}

func processRow(text string) (string, string, string) {
	rows := strings.Split(text, "|")

	var firstName, fullName, month string

	fullName = strings.Replace(strings.TrimSpace(rows[7]), " ", "", -1)

	name := strings.TrimSpace(rows[7])

	if name != "" {
		startOfName := strings.Index(name, ", ") + 2
		if endOfName := strings.Index(name[startOfName:], " "); endOfName < 0 {
			firstName = name[startOfName:]
		} else {
			firstName = name[startOfName : startOfName+endOfName]
		}

		if strings.HasSuffix(firstName, ",") {
			firstName = strings.Replace(firstName, ",", "", -1)
		}
	}

	date := strings.TrimSpace(rows[13])

	if len(date) == 8 {
		month = date[:2]
	} else {
		month = "--"
	}

	return firstName, fullName, month
}

func Sequential(file string) result {
	res := result{
		donationMonthFreq: map[string]int{},
	}

	f, err := os.Open(file)
	if err != nil {
		log.Fatalf("Open file err: %v", err)
	}

	fullNameRegister := make(map[string]bool)

	firstNameMap := make(map[string]int)

	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		row := scanner.Text()

		firstName, fullName, month := processRow(row)

		fullNameRegister[fullName] = true

		firstNameMap[firstName]++

		if firstNameMap[firstName] > res.commonNameCount {
			res.commonName = firstName
			res.commonNameCount = firstNameMap[firstName]
		}

		res.donationMonthFreq[month]++

		res.numRows++
		res.peopleCount = len(fullNameRegister)
	}

	return res
}

func reader(ctx context.Context, f *os.File, batchSize int) <-chan []string {
	out := make(chan []string)

	scaner := bufio.NewScanner(f)
	var batchRows []string

	go func() {
		defer close(out)

		for {
			scanned := scaner.Scan()
			select {
			case <-ctx.Done():
				return
			default:
				row := scaner.Text()

				if len(batchRows) == batchSize || !scanned {
					out <- batchRows
					batchRows = []string{}
				}
				batchRows = append(batchRows, row)
			}

			if !scanned {
				return
			}
		}
	}()

	return out
}

func worker(ctx context.Context, rowBatch <-chan []string, batchSize int) <-chan processed {
	out := make(chan processed)

	go func() {
		defer close(out)

		p := processed{
			firstNames: make([]string, 0, batchSize),
			fullNames:  make([]string, 0, batchSize),
			months:     make([]string, 0, batchSize),
		}

		for rb := range rowBatch {
			for _, row := range rb {
				firstName, fullName, month := processRow(row)
				p.firstNames = append(p.firstNames, firstName)
				p.fullNames = append(p.fullNames, fullName)
				p.months = append(p.months, month)
			}
		}
		out <- p
	}()

	return out
}

func multiplexer(ctx context.Context, wg *sync.WaitGroup, in <-chan processed, out chan processed) {
	defer wg.Done()

	for p := range in {
		select {
		case <-ctx.Done():
			return
		case out <- p:
		}
	}
}

func combiner(ctx context.Context, inputs ...<-chan processed) <-chan processed {
	out := make(chan processed)

	var wg sync.WaitGroup
	wg.Add(len(inputs))
	for _, in := range inputs {
		go multiplexer(ctx, &wg, in, out)
	}

	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func Concurrent(file string, numWorkers, batchSize int) result {
	res := result{
		donationMonthFreq: map[string]int{},
	}

	f, err := os.Open(file)
	if err != nil {
		log.Fatalf("Open file err: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rowsCh := reader(ctx, f, batchSize)

	workerCh := make([]<-chan processed, numWorkers)
	for i := 0; i < numWorkers; i++ {
		workerCh[i] = worker(ctx, rowsCh, batchSize)
	}

	firstNameCount := map[string]int{}
	fullNameCount := map[string]bool{}

	for processed := range combiner(ctx, workerCh...) {
		res.numRows += processed.numRows

		for _, month := range processed.months {
			res.donationMonthFreq[month]++
		}

		for _, fullName := range processed.fullNames {
			fullNameCount[fullName] = true
		}

		res.peopleCount = len(fullNameCount)

		for _, firstName := range processed.firstNames {
			firstNameCount[firstName]++

			if firstNameCount[firstName] > res.commonNameCount {
				res.commonName = firstName
				res.commonNameCount = firstNameCount[firstName]
			}
		}
	}

	return res
}
