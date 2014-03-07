package raft

import (
	"bufio"
	"fmt"
	cluster "github.com/nileshjagnik/kv_store/asgn2"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

type raftServer interface {
	Id() int
	Term() int
	SetTerm(t int)
	isLeader() bool
	SetLeader(b bool)
	isHealthy() bool
	SetHealthy(b bool)
	votedFor() int
	SetVotedFor(i int)
	VoteCount() int
	SetVoteCount(v int)
	Close()
	Start()
}

type node struct {
	id        int
	totNodes  int
	term      int
	isleader  bool
	ishealthy bool
	votedfor  int
	voteCount int
	quit chan bool
	server cluster.Server
	debug     int
}

func (n *node) Id() int {
	return n.id
}
func (n *node) Term() int {
	return n.term
}
func (n *node) SetTerm(t int) {
	n.term = t
}
func (n *node) isLeader() bool {
	return n.isleader
}
func (n *node) SetLeader(b bool) {
	n.isleader = b
}
func (n *node) isHealthy() bool {
	return n.ishealthy
}
func (n *node) SetHealthy(b bool) {
	n.ishealthy = b
}
func (n *node) votedFor() int {
	return n.votedfor
}
func (n *node) SetVotedFor(i int) {
	n.votedfor = i
}
func (n *node) VoteCount() int {
	return n.voteCount
}
func (n *node) SetVoteCount(v int) {
	n.voteCount = v
}
func (n *node) Close(){
	n.quit <- true
}
func (n *node) Start(){
        n.term = -1
        n.isleader = false
        n.ishealthy = true
        n.votedfor = -1
        n.voteCount = 0
        go RunForever(n,n.id,n.totNodes,n.debug)
}

func NewServer(id int, confile string) raftServer {
        debug := 0
	total := 0
	conf, er := os.Open(confile)
	if er != nil {
		log.Fatal(er)
	}
	inp := bufio.NewScanner(conf)
	for inp.Scan() {
		inp.Text()
		total += 1
	}
	serv := cluster.NewServer(id, confile)
	new_serv := node{id, total, -1, false, true, -1, 0,make(chan bool),serv,debug}
	go RunForever(&new_serv,id,total,debug) 
	return &new_serv
}


func RunForever(new_serv *node, id int, total int, debug int) {
        for {
                select {
                case msg := <-new_serv.quit:
                        if msg == true {
		                if debug == 1 {
		                        fmt.Printf("Quitting new_serv.server: %d\n", id)
	                        }
		                return
	                }

                default:
                        //PrintState(new_serv.isLeader())
	                if new_serv.isHealthy() == true {
		                var msg string
		                if new_serv.isLeader() == false {
			                tim, _ := time.ParseDuration(strconv.Itoa(400+rand.Intn(401)) + "ms")
			                select {
			                
			                case envelope := <-new_serv.server.Inbox():
			                        message := envelope.Msg.(string)
		                                rpctype := strings.Split(message, " ")[0]
		                                msgfrom, _ := strconv.Atoi(strings.Split(message, " ")[2])
			                        msgterm, _ := strconv.Atoi(strings.Split(message, " ")[1])
			                        if msgterm >= new_serv.Term() {
			                                if(msgterm > new_serv.Term()) {
			                                        BecomeFollower(new_serv,msgterm)
			                                        
			                                }
				                        if rpctype == "AppendEntries" {
				                                if debug == 1 {
					                                fmt.Printf("new_serv.server %d says:", id)
					                                fmt.Printf("Got an AppendEntries RPC : %s\n", message)
				                                }
					                        BecomeFollower(new_serv,msgterm)
					                        
				                        } else if rpctype == "RequestVote" {
				                                if debug == 1 {
					                                fmt.Printf("new_serv.server %d says:", id)
					                                fmt.Printf("Got a RequestVote RPC: %s\n", message)
				                                }
					                        if new_serv.votedFor() == -1 {
						                        msg = "CastVote" + " " + strconv.Itoa(new_serv.Term()) + " " + strconv.Itoa(id) + " nil"
						                        new_serv.server.Outbox() <- &cluster.Envelope{Pid: msgfrom, Msg: msg}
						                        new_serv.SetVotedFor(msgfrom)
						                        if debug == 1 {
						                                fmt.Printf("new_serv.server %d says:", id)
						                                fmt.Printf("Sending CastVote to : %d\n", msgfrom)
					                                }
					                        }
					                        new_serv.SetTerm(msgterm)
				                        } else if rpctype == "CastVote" {
					                        new_serv.SetVoteCount(new_serv.VoteCount()+1)
					                        if debug == 1 {
					                                fmt.Printf("new_serv.server %d says:", id)
					                                fmt.Printf("Got a CastVote RPC from %d, I now have %d : votes \n", msgfrom, new_serv.VoteCount())
				                                }
					                        if 2*new_serv.VoteCount() > total {
						                        new_serv.SetLeader(true)
						                        if debug == 1 {
						                                fmt.Printf("new_serv.server %d says:", id)
						                                fmt.Printf("I am leader now HUHUHAHA!\n")
					                                }
					                        }
				                        }
				                }
			                case <-time.After(tim):
			                        if debug == 1 {
				                        fmt.Printf("new_serv.server %d says:", id)
				                        fmt.Printf("Need new election\n")
			                        }
				                new_serv.SetVoteCount(1)
				                new_serv.SetVotedFor(id)
				                new_serv.SetTerm(new_serv.Term() + 1)
				                msg = "RequestVote" + " " + strconv.Itoa(new_serv.Term()) + " " + strconv.Itoa(id) + " nil"
				                new_serv.server.Outbox() <- &cluster.Envelope{Pid: cluster.BROADCAST, Msg: msg}

			                }
		                } else {
			                select {
			                case envelope := <-new_serv.server.Inbox():
			                        message := envelope.Msg.(string)
			                        msgterm, _ := strconv.Atoi(strings.Split(message, " ")[1])
			                        if(msgterm > new_serv.Term()) {
		                                        BecomeFollower(new_serv,msgterm)
		                                }
			                default:
			                        if debug == 1 {
				                        fmt.Printf("Your leader new_serv.server %d sends everyone an AppendEntries RPC\n", id)
			                        }
				                new_serv.SetTerm(new_serv.Term() + 1)
				                msg = "AppendEntries "
				                msg += strconv.Itoa(new_serv.Term())
				                msg += " " + strconv.Itoa(id) + " nil"
				                new_serv.server.Outbox() <- &cluster.Envelope{Pid: cluster.BROADCAST, Msg: msg}
				                time.Sleep(400*time.Millisecond)
			                }
		                }
	                }
	        }
        }
}

func PrintState(b bool){
        var state string
        if b == true {
                state="leader"
        } else {
                state="follower"
        }
        fmt.Printf("%s\n",state)
}
func BecomeFollower(new_serv *node, msgterm int){
        new_serv.SetTerm(msgterm)
        new_serv.SetVotedFor(-1)
        new_serv.SetVoteCount(0)
        new_serv.SetLeader(false)
}
