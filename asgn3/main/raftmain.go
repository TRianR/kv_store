package main

import (
	raft "github.com/nileshjagnik/kv_store/asgn3/lib"
	"math/rand"
	"time"
	"flag"
	"fmt"
)

func main() {
        rand.Seed(time.Now().UTC().UnixNano())
        id := flag.Int("id", 1, "This is the ID")
        flag.Parse()
	raft.NewServer(*id, "config.txt")
	select {
	        case <-time.After(200*time.Second):
	        fmt.Printf("Time's up!\n")
	}
}
