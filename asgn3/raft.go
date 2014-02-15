package main

import (
        "bufio"
	//"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
	"math/rand"
	cluster "github.com/nileshjagnik/kv_store/asgn2"
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
}

type node struct {
        id int
        term int
        isleader bool
        ishealthy bool
        votedfor int
        voteCount int
}
func (n *node) Id() int {
        return n.id
}
func (n *node) Term() int {
        return n.term
}
func (n *node) SetTerm(t int)  {
        n.term = t
}
func (n *node) isLeader() bool {
        return n.isleader
}
func (n *node) SetLeader(b bool)  {
        n.isleader = b
}
func (n *node) isHealthy() bool {
        return n.ishealthy
}
func (n *node) SetHealthy(b bool)  {
        n.ishealthy = b
}
func (n *node) votedFor() int {
        return n.votedfor
}
func (n *node) SetVotedFor(i int) {
        n.votedfor = i 
}
func (n *node) VoteCount() int{
        return n.voteCount 
}
func (n *node) SetVoteCount(v int) {
        n.voteCount = v
}

/*
var conf
        var er
        conf, er = os.Open("termfile")
	if er == nil {
	        inp := bufio.NewScanner(conf)
	        term_no = strconv.Atoi(inp.Text())
		leader = false;
	} else {
	        term_no = 1
	        err := conf.Close()
	        conf = os.Create("termfile")
	        conf.WriteString(Itoa(term_no))
	        err = conf.Close()
	        leader = true;
	}
*/

func NewServer(id int, confile string) raftServer {
        new_serv := node{id, -1, false, true, -1, 0}
        total := 0
        conf, er := os.Open(confile)
	if er != nil {
		log.Fatal(er)
	}
	inp := bufio.NewScanner(conf)
	for inp.Scan() {
	        inp.Text()
	        total +=1
        }
        server := cluster.NewServer(id,confile)
        go func() {
                for {
                        if(new_serv.isHealthy() == true) {
                                var msg string
                                if (new_serv.isLeader() == false) {
                                        rand.Seed( time.Now().UTC().UnixNano())
                                        tim,_ := time.ParseDuration(strconv.Itoa(2 + rand.Intn(3))+"s")
                                        select {
                                                case envelope := <- server.Inbox():
                                                        message := envelope.Msg.(string)
                                                        //fmt.Printf("server %d says:", id)
                                                        rpctype := strings.Split(message, " ")[0]
                                                        //fmt.Printf("Got an RPC :%s\n", rpctype)
                                                        
                                                        
                                                        msgfrom,_ := strconv.Atoi(strings.Split(message, " ")[2])
                                                        if (rpctype == "AppendEntries") {
                                                                new_serv.SetVotedFor(-1)
                                                                fmt.Printf("server %d says:", id)
                                                                fmt.Printf("Got an AppendEntries RPC : %s\n", message)
                                                                msgterm,_ := strconv.Atoi(strings.Split(message, " ")[1])
                                                                if (msgterm > new_serv.Term()) {
                                                                        new_serv.SetTerm(msgterm)
                                                                }
                                                        } else if (rpctype == "RequestVote") {
                                                                fmt.Printf("server %d says:", id)
                                                                fmt.Printf("Got a RequestVote RPC: %s\n", message)
                                                                if (new_serv.votedFor() == -1 ) {
                                                                        msg = "CastVote" + " " + strconv.Itoa(new_serv.Term()) + " " + strconv.Itoa(id) +" nil"
                                                                        server.Outbox() <- &cluster.Envelope{Pid: msgfrom, Msg: msg}
                                                                        new_serv.SetVotedFor(msgfrom)
                                                                        fmt.Printf("server %d says:", id)
                                                                        fmt.Printf("Sending CastVote to : %d\n", msgfrom)
                                                                }
                                                                msgterm,_ := strconv.Atoi(strings.Split(message, " ")[1])
                                                                if (msgterm > new_serv.Term()) {
                                                                        new_serv.SetTerm(msgterm)
                                                                }
                                                        } else if (rpctype == "CastVote") {
                                                                cur := new_serv.VoteCount()
                                                                cur += 1
                                                                new_serv.SetVoteCount(cur)
                                                                fmt.Printf("server %d says:", id)
                                                                fmt.Printf("Got a CastVote RPC from %d, I now have %d : votes \n",msgfrom, new_serv.VoteCount())
                                                                if (2 * cur > total) {
                                                                        new_serv.SetLeader(true)
                                                                        fmt.Printf("server %d says:", id)
                                                                        fmt.Printf("I am leader now HUHUHAHA!\n")
                                                                }
                                                        }
                                                case <- time.After(tim):
                                                        fmt.Printf("server %d says:", id)
                                                        fmt.Printf("Need new election\n")
                                                        new_serv.SetVoteCount(1)
                                                        msg = "RequestVote" + " " + strconv.Itoa(new_serv.Term()) + " " + strconv.Itoa(id) +" nil"
                                                        server.Outbox() <- &cluster.Envelope{Pid: cluster.BROADCAST, Msg: msg}
                                                        
                                        }
                                } else {
                                        select {
                                                case <- server.Inbox():
                                                default :
                                                        fmt.Printf("Your leader server %d sends everyone an AppendEntries RPC\n", id)
                                                        new_serv.SetTerm(new_serv.Term()+1)
                                                        msg = "AppendEntries "
                                                        msg += strconv.Itoa(new_serv.Term())
                                                        msg += " " + strconv.Itoa(id) +" nil"
                                                        server.Outbox() <- &cluster.Envelope{Pid: cluster.BROADCAST, Msg: msg}
                                                        time.Sleep(time.Second)
                                                }
                                        
                                }
                        }          
                }
        }()
        return &new_serv
}

func main() {
        	var svrArr []raftServer
                for i := 1; i < 6; i++ {
                svrArr = append(svrArr, NewServer(i, "config.txt"))
                }
        	select {
                        case <-time.After(20 * time.Second):
                        println("Waited and waited. Ab thak gaya\n")
                }
}
