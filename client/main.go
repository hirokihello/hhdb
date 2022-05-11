package main

import (
	"bufio"
	"fmt"
	"os"
)

func main() {
		// const url_string string= "hhdb://localhost/Users/hiroki/git/github.com/hirokihello/hhdb";
    // fmt.Println("db connected");
    for {
        fmt.Println("Enter some words!")
        input := bufio.NewScanner(os.Stdin)
        input.Scan()
        fmt.Println("input is " + input.Text())
    }
}