package main

import (
	"fmt"
	"time"
)

func main() {
	now := time.Now()
	// res := Sequential("./data/real.txt")

	res := Concurrent("./data/real.txt", 10, 10000)

	fmt.Println(time.Since(now).Milliseconds())
	fmt.Printf("%+v\n", res)
}
