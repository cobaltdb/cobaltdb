package main

import (
	"os"
	"fmt"
)

func main() {
	fmt.Println("gen")
	_ = os.WriteFile("_test.txt", []byte("ok"), 0644)
}
