package cluster

import (
	"bufio"
	"encoding/json"
	"fmt"
	zmq4 "github.com/pebbe/zmq4"
	"log"
	"os"
	"strconv"
	"strings"
	//"time"
)

const (
	BROADCAST = -1
)

type Envelope struct {
	// On the sender side, Pid identifies the receiving peer. If instead, Pid is
	// set to cluster.BROADCAST, the message is sent to all peers. On the receiver side, the
	// Id is always set to the original sender. If the Id is not found, the message is silently dropped
	Pid int

	// An id that globally and uniquely identifies the message, meant for duplicate detection at
	// higher levels. It is opaque to this package.
	MsgId int64

	// the actual message.
	Msg interface{}
	
	TrialMsg interface{}
}

type Server interface {
	// Id of this server
	Pid() int

	// array of other servers' ids in the same cluster
	Peers() []int

	// the channel to use to send messages to other peers
	// Note that there are no guarantees of message delivery, and messages
	// are silently dropped
	Outbox() chan *Envelope

	// the channel to receive messages from other peers.
	Inbox() chan *Envelope
}

type node struct {
	pid     int
	peers   []int
	address []string
	inchan  chan *Envelope
	outchan chan *Envelope
}

func (n *node) Pid() int {
	return n.pid
}

func (n *node) Peers() []int {
	return n.peers
}

func (n *node) Inbox() chan *Envelope {
	return n.inchan
}

func (n *node) Outbox() chan *Envelope {
	return n.outchan
}

func NewServer(id int, file string) Server {
	conf, er := os.Open(file)
	if er != nil {
		log.Fatal(er)
	}
	var buffer string
	var th_id int
	var th_add string
	var idArr []int
	var addArr []string
	var hostname string
	inp := bufio.NewScanner(conf)
	for inp.Scan() {
		buffer = inp.Text()
		th_id, er = strconv.Atoi(strings.Split(buffer, " ")[0])
		th_add = strings.Split(buffer, " ")[1]
		if th_id != id {
			idArr = append(idArr, th_id)
			addArr = append(addArr, th_add)
		} else {
			hostname = th_add
		}
	}
	new_serv := node{id, idArr, addArr, make(chan *Envelope), make(chan *Envelope)}

	go func() {
		receiver, er := zmq4.NewSocket(zmq4.PULL)
		defer receiver.Close()

		if er != nil {
			fmt.Printf("Error creating socket in receiver, %s: %s\n", hostname, er)
			os.Exit(1)
		} else {
			//fmt.Printf("created socket in receiver, %s\n", hostname)
		}
		er = receiver.Bind(hostname)
		if er != nil {
			fmt.Printf("Error binding socket in receiver, %s: %s\n", hostname, er)
			os.Exit(1)
		} else {
			//fmt.Printf("bound socket in receiver, %s\n", hostname)
		}
		for {
			msgbytes, er := receiver.RecvBytes(0)
			if er != nil {
				fmt.Printf("Error recieving message in receiver, %s: %s\n", hostname, er)
				os.Exit(2)
			}
			//fmt.Printf("Received Msg in receiver, %d : %s\n", id, string(msgbytes))
			var msg Envelope
			json.Unmarshal(msgbytes, &msg)
			new_serv.Inbox() <- &msg
			//time.Sleep(time.Second)
		}
	}()

	go func() {
		sender, _ := zmq4.NewSocket(zmq4.PUSH)
		defer sender.Close()
		if er != nil {
			fmt.Printf("Error creating socket in sender %s: %s\n", hostname, er)
			os.Exit(1)
		} else {
			//fmt.Printf("Created socket in sender %s\n", hostname)
		}
		for {
			send := <-new_serv.Outbox()
			if send != nil {
				serverid := send.Pid
				send.Pid = id
				var host string
				msg, _ := json.Marshal(send)
				if serverid != -1 {
					for i := range idArr {
						if idArr[i] == serverid {
							host = addArr[i]
							sender.Connect(host)
							sender.SendBytes(msg, 0)
							sender.Disconnect(host)
							//fmt.Printf("Sent msg from %s to %s\n", hostname, host)
							break
						}
					}
				} else {
					for i := range idArr {
						host = addArr[i]
						sender.Connect(host)
						sender.SendBytes(msg, 0)
						sender.Disconnect(host)
						//fmt.Printf("Sent msg from %s to %s\n", hostname, host)
						//time.Sleep(2 * time.Second)
					}
				}
			}
		}
	}()
	fmt.Printf("Created Node: %s\n", hostname)
	return &new_serv
}
