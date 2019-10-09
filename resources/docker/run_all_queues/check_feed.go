package main

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strconv"
)

func main() {
	var values []int

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		valStr := scanner.Text()
		val, err := strconv.Atoi(valStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			continue
		}
		values = append(values, val)
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading standard input:", err)
	}

	sort.Ints(values)
	for i, v := range values {
		if i == 0 {
			continue
		}
		prev := values[i-1]
		if v == prev {
			fmt.Printf("Duplicate found: %v\n", v)
		} else if prev < (v - 1) {
			fmt.Printf("Missing values detected between %v and %v\n", prev, v)
		}
	}
}
