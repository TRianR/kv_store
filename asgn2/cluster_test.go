package cluster

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)
func TestMain(t *testing.T) {
	a := make([][]int, 5)
	for i := range a {
		a[i] = make([]int, 5)
	}

	for i := 0; i < 5; i++ {
		for j := 0; j < 5; j++ {
			a[i][j] = 0
		}
	}
	var svrArr []Server

	for i := 1; i < 6; i++ {
		svrArr = append(svrArr, NewServer(i /* config file */, "config.txt"))
	}

	time.Sleep(time.Second)

	go func() {
		for j := 0; j < 10; j++ {
			for i := 1; i < 6; i++ {
				svrArr[i-1].Outbox() <- &Envelope{Pid: BROADCAST, Msg: strconv.Itoa(i)}
			}
			time.Sleep(1 * time.Second)
		}
	}()

	go func() {
		for {
			select {
			case envelope := <-svrArr[0].Inbox():
				from, _ := strconv.Atoi(envelope.Msg.(string))
				a[from-1][0] += 1
			case envelope := <-svrArr[1].Inbox():
				from, _ := strconv.Atoi(envelope.Msg.(string))
				a[from-1][1] += 1
			case envelope := <-svrArr[2].Inbox():
				from, _ := strconv.Atoi(envelope.Msg.(string))
				a[from-1][2] += 1
			case envelope := <-svrArr[3].Inbox():
				from, _ := strconv.Atoi(envelope.Msg.(string))
				a[from-1][3] += 1
			case envelope := <-svrArr[4].Inbox():
				from, _ := strconv.Atoi(envelope.Msg.(string))
				a[from-1][4] += 1
			}
		}
	}()

	select {
	case <-time.After(25 * time.Second):
		var flag bool
		flag = true
		msgval := 0
		for i := 0; i < 5; i++ {
			for j := 0; j < 5; j++ {
				fmt.Printf("%d\t", a[i][j])
			}
			fmt.Printf("\n")
		}
		for i := 0; i < 5; i++ {
			for j := 0; j < 5; j++ {
				if (a[i][j] != 10) && (i != j) {
					msgval = a[i][j]
					flag = false
					break
				}
			}
		}
		if flag == true {
			println("Testing successful\n")
		} else {
			fmt.Printf("Testing Unsucessful: %d\n", msgval)
			t.Error()
		}
		println("Ending testing\n")
	}

}
