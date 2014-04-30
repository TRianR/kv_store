package raft

import (
	"bufio"
	"fmt"
	cluster "github.com/nileshjagnik/kv_store/asgn2"
	sm "github.com/nileshjagnik/kv_store/StateMachine"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

// Identifies an entry in the log
type LogItem struct {
	// An index into an abstract 2^64 size array
	Index int64

	Term int

	// The data that was supplied to raft's inbox
	Data interface{}
}

var index int64

type Rpc struct {
	RpcType      string
	Term         int
	FromId       int
	LastIndex    int
	LastTerm     int
	LeaderCommit int
	Entries      []LogItem
	Result       string
}

type raftServer interface {
	Id() int
	Leader() int
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
	Outbox() chan<- interface{}
	Inbox() <-chan *LogItem
	ClientSideOutbox() <-chan interface{}
}

type node struct {
	id          int
	leader      int
	totNodes    int
	term        int
	isleader    bool
	ishealthy   bool
	votedfor    int
	voteCount   int
	quit        chan bool
	server      cluster.Server
	stmachine   sm.StateMachine
	debug       int
	inchan      chan *LogItem
	outchan     chan interface{}
	clioutchan  chan interface{}
	Log         []LogItem
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	//timers      []time.Timer
}

func (n *node) Id() int {
	return n.id
}

func (n *node) Leader() int {
	return n.leader
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
func (n *node) Close() {
	n.quit <- true
}
func (n *node) Start() {
	n.term = -1
	n.isleader = false
	n.ishealthy = true
	n.votedfor = -1
	n.voteCount = 0
	go RunForever(n, n.totNodes, n.debug)
}
func (n *node) Outbox() chan<- interface{} {
	return n.outchan
}
func (n *node) ClientSideOutbox() <-chan interface{} {
	return n.clioutchan
}
func (n *node) Inbox() <-chan *LogItem {
	return n.inchan
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
	statemachine := sm.NewStatemachine(id)
	var Log []LogItem
	new_serv := node{id, -1, total, 0, false, true, -1, 0, make(chan bool), serv, statemachine, debug, make(chan *LogItem), 
	make(chan interface{}), make(chan interface{}), Log, 0, 0, make([]int, total), make([]int, total)}
	go RunForever(&new_serv, total, debug)
	return &new_serv
}

func RunForever(new_serv *node, total int, debug int) {
                tim, _ := time.ParseDuration(strconv.Itoa(400+rand.Intn(401)) + "ms")
		timer := time.NewTimer(tim)
	        tim2, _ := time.ParseDuration(strconv.Itoa(400) + "ms")
		timer2 := time.NewTimer(tim)
	for {
		//var msg string
		if new_serv.isLeader() == false {
		        
			// If commitIndex > lastApplied: increment lastApplied, apply  log[lastApplied] to state machine
			if new_serv.commitIndex > new_serv.lastApplied {
			        //fmt.Printf("Follower result ready")
				new_serv.lastApplied++
				new_serv.inchan <- &new_serv.Log[new_serv.lastApplied-1]
				var tokens []string
				//result := "Failure"
				tokens = strings.Split(new_serv.Log[new_serv.lastApplied].Data.(string)," ")
				if (tokens[0]=="get") {
				        //result = new_serv.stmachine.Get(tokens[1])
				} else if (tokens[0] == "set") {
				        new_serv.stmachine.Set(tokens[1], tokens[2])
				        //result = "Sucess"
				}
				//new_serv.clioutchan <- result
			}

			
			select {
			case msg := <-new_serv.quit:
				if msg == true {
					if debug == 1 {
						fmt.Printf("Quitting new_serv.server: %d\n", new_serv.id)
					}
					return
				}
			case <-new_serv.outchan:
				//fmt.Printf("Request Failed: Leader is %d", new_serv.Leader())
				new_serv.clioutchan <- "Request Failed: Leader is " +  strconv.Itoa(new_serv.Leader())
				
			case envelope := <-new_serv.server.Inbox():
				//Respond to RPCs from candidates and leaders
				var rpctype, result string
				var Entries []LogItem
				var msgfrom, msgterm, LastIndex, LastTerm, LeaderCommit int
				switch envelope.Msg.(type) {
				case string:
					message := envelope.Msg.(string)
					rpctype = strings.Split(message, " ")[0]
					msgfrom, _ = strconv.Atoi(strings.Split(message, " ")[2])
					msgterm, _ = strconv.Atoi(strings.Split(message, " ")[1])
				case map[string]interface{}:
					mes := envelope.Msg.(map[string]interface{})
					rpctype = mes["RpcType"].(string)
					result = mes["Result"].(string)
					msgfrom = int(mes["FromId"].(float64))
					msgterm = int(mes["Term"].(float64))
					LastIndex = int(mes["LastIndex"].(float64))
					LastTerm = int(mes["LastTerm"].(float64))
					LeaderCommit = int(mes["LeaderCommit"].(float64))
					if mes["Entries"] == nil {
						Entries = nil
					} else {
						switch mes["Entries"].(type) {
						case []interface{}:
							vals := mes["Entries"].([]interface{})
							for _, val := range vals {
								realval := val.(map[string]interface{})
								Entries = append(Entries, LogItem{Index: int64(realval["Index"].(float64)),
									Term: int(realval["Term"].(float64)), Data: realval["Data"]})
							}
						default:
							fmt.Printf("Error parsing message, server %d\n", new_serv.id)
						}
					}
				}
				if msgterm >= new_serv.Term() {
					//If RPC request or response contains term T > currentTerm:  set currentTerm = T, convert to follower
                                        timer.Reset(tim)
					if msgterm > new_serv.Term() {
						BecomeFollower(new_serv, msgterm, msgfrom)

					}
					if rpctype == "AppendEntries" {
						if debug == 1 {
							fmt.Printf("server %d says:", new_serv.id)
							fmt.Printf("Got an AppendEntries RPC :\n")
							fmt.Println(envelope.Msg)
						}
						BecomeFollower(new_serv, msgterm, msgfrom)
						if Entries != nil {
						        //fmt.Println(Entries)
							//2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
							if LastIndex == 0 {
							        //fmt.Println("0")
								if len(new_serv.Log) == 0 {
								        var logit []LogItem
									new_serv.Log = logit
									for _, val := range Entries {
										new_serv.Log = append(new_serv.Log, val)
									}
									//fmt.Println(new_serv.Log)
									/*
									RpcType      string
	                                                                Term         int
	                                                                FromId       int
	                                                                LastIndex    int
	                                                                LastTerm     int
	                                                                LeaderCommit int
	                                                                Entries      []LogItem
	                                                                Result       string
	                                                                */
									rp := Rpc{"AppendEntriesResponse", new_serv.Term(), new_serv.id, len(new_serv.Log) + 1,
										len(new_serv.Log), -1, nil, "true"}
									new_serv.server.Outbox() <- &cluster.Envelope{Pid: msgfrom, Msg: rp}
								}
							} else if (len(new_serv.Log) < LastIndex) || (new_serv.Log[LastIndex-1].Term != LastTerm) {
							//fmt.Println("1")
							        //fmt.Printf("len(new_serv.Log) - %d, LastIndex - %d\n", len(new_serv.Log), LastIndex)
								var entries []LogItem
								rp := Rpc{"AppendEntriesResponse", new_serv.Term(), new_serv.id, -1, -1, -1, entries, "false"}
								new_serv.server.Outbox() <- &cluster.Envelope{Pid: msgfrom, Msg: rp}
							} else {
							//fmt.Println("2")
								/*
									3. If an existing entry conflicts with a new one (same index but different terms),
									delete the existing entry and all that follow it
									4. Append any new entries not already in the log*/

								entries := new_serv.Log[:LastIndex-1]
								for _, val := range Entries {
									entries = append(entries, val)
								}
								new_serv.Log = make([]LogItem, 1000)
								for _, val := range entries {
									new_serv.Log = append(new_serv.Log, val)
								}
								rp := Rpc{"AppendEntriesResponse", new_serv.Term(), new_serv.id, len(new_serv.Log) + 1,
									new_serv.Log[len(new_serv.Log)-1].Term, -1, nil, "true"}
								//fmt.Println(new_serv.Log)
								new_serv.server.Outbox() <- &cluster.Envelope{Pid: msgfrom, Msg: rp}
								//fmt.Println(new_serv.Log)
							}
							//5.If leaderCommit > set, commitIndex commitIndex = min(leaderCommit, last log index)
							last := len(new_serv.Log)
							if LeaderCommit > new_serv.commitIndex {
								if LeaderCommit < last {
									new_serv.commitIndex = LeaderCommit
								} else {
									new_serv.commitIndex = last
								}
							}
						}

					} else if rpctype == "RequestVote" {
						if debug == 1 {
							fmt.Printf("server %d says:", new_serv.id)
							fmt.Printf("Got a RequestVote RPC: \n")
							fmt.Println(envelope.Msg)
						}
						flag := 0
						last := len(new_serv.Log)
						if last == 0 {
							flag = 1
						} else if !(new_serv.Log[last-1].Term > LastTerm) || (new_serv.Log[last-1].Term == LastTerm) && (last > LastIndex) {
							//To check that candidate's log is atleast as up-to-date as receiver’s log
							flag = 1
						}
						if (new_serv.votedFor() == -1) && (flag == 1) {
							//2.If votedFor is null or candidateId, and candidate's log is atleast as up-to-date as receiver’s log, grant vote

							var entries []LogItem
							rp := Rpc{"RequestVoteResponse", new_serv.Term(), new_serv.id, -1, -1, -1, entries, "true"}
							new_serv.server.Outbox() <- &cluster.Envelope{Pid: msgfrom, Msg: rp}
							new_serv.SetVotedFor(msgfrom)
							if debug == 1 {
								fmt.Printf("server %d says:", new_serv.id)
								fmt.Printf("Sending CastVote to : %d\n", msgfrom)
							}
							new_serv.SetTerm(msgterm)
						} else {
							//else reject vote
							var entries []LogItem
							rp := Rpc{"RequestVoteResponse", new_serv.Term(), new_serv.id, -1, -1, -1, entries, "false"}
							new_serv.server.Outbox() <- &cluster.Envelope{Pid: msgfrom, Msg: rp}
						}

					} else if rpctype == "RequestVoteResponse" {
						if result == "true" {
							new_serv.SetVoteCount(new_serv.VoteCount() + 1)
							if debug == 1 {
								fmt.Printf("server %d says:", new_serv.id)
								fmt.Printf("Got a CastVote RPC from %d, I now have %d : votes \n", msgfrom, new_serv.VoteCount())
							}
							if 2*new_serv.VoteCount() > total {
								//If votes received from majority of servers: become leader
								new_serv.leader = new_serv.id
								new_serv.SetLeader(true)
								//for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
								new_serv.nextIndex = make([]int, total)
								for i := 0; i < total; i++ {
									new_serv.nextIndex = append(new_serv.nextIndex, (len(new_serv.Log)+1))
								}
								for i := 0; i < total; i++ {
									new_serv.nextIndex[i] = (len(new_serv.Log)+1)
								}
								
								//for each server, index of highest log entry known to be replicated on server
								new_serv.matchIndex = make([]int, total)
								for i := 0; i < total; i++ {
									new_serv.matchIndex = append(new_serv.matchIndex, 0)
								}
								for i := 0; i < total; i++ {
									new_serv.matchIndex[i] = 0
								}
								if debug == 1 {
									fmt.Printf("server %d says:", new_serv.id)
									fmt.Printf("I am leader now HUHUHAHA!\n")
								}
							}
						}
					}
				} else {
					if rpctype == "AppendEntries" {
						//1.Reply false if term < currentTerm
						if debug == 1 {
							fmt.Printf("server %d says:", new_serv.id)
							fmt.Printf("Got an AppendEntries RPC :\n")
							fmt.Println(envelope.Msg)
						}
						var entries []LogItem
						rp := Rpc{"AppendEntriesResponse", new_serv.Term(), new_serv.id, -1, -1, -1, entries, "false"}
						new_serv.server.Outbox() <- &cluster.Envelope{Pid: msgfrom, Msg: rp}
					} else if rpctype == "RequestVote" {
						//1. Reply false if term < currentTerm
						var entries []LogItem
						rp := Rpc{"RequestVoteResponse", new_serv.Term(), new_serv.id, -1, -1, -1, entries, "false"}
						new_serv.server.Outbox() <- &cluster.Envelope{Pid: msgfrom, Msg: rp}
					}
				}
			case <-timer.C:
				//If election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate: convert to candidate
				/*On conversion to candidate, start election:
				  • Increment currentTerm
				  • Vote for self
				  • Reset election timeout
				  • Send RequestVote RPCs to all other servers */
				if debug == 1 {
					fmt.Printf("server %d says:", new_serv.id)
					fmt.Printf("Need new election\n")
				}
				new_serv.SetVoteCount(1)
				new_serv.SetVotedFor(new_serv.id)
				new_serv.SetTerm(new_serv.Term() + 1)
				var entries []LogItem
				ln := len(new_serv.Log)
				if ln != 0 {
					rp := Rpc{"RequestVote", new_serv.Term(), new_serv.id, ln, new_serv.Log[ln-1].Term,
						new_serv.commitIndex, entries, "true"}
					new_serv.server.Outbox() <- &cluster.Envelope{Pid: cluster.BROADCAST, Msg: rp}
				} else {
					rp := Rpc{"RequestVote", new_serv.Term(), new_serv.id, ln, -1,
						new_serv.commitIndex, entries, "true"}
					new_serv.server.Outbox() <- &cluster.Envelope{Pid: cluster.BROADCAST, Msg: rp}
				}
				tim, _ = time.ParseDuration(strconv.Itoa(400+rand.Intn(401)) + "ms")
				timer.Reset(tim)

			}
		} else {
			// If commitIndex > lastApplied: increment lastApplied, apply  log[lastApplied] to state machine
			if new_serv.commitIndex > new_serv.lastApplied {
			        //fmt.Printf("Leader result ready\n")
				new_serv.lastApplied++
				//fmt.Println(new_serv.Log[new_serv.lastApplied-1].Data.(string))
				//new_serv.inchan <- &new_serv.Log[new_serv.lastApplied-1]
				
				var tokens []string
				result := "Failure"
				
				tokens = strings.Split(new_serv.Log[new_serv.lastApplied-1].Data.(string)," ")
				//fmt.Println(tokens)
				
				if (tokens[0]=="get") {
				        result = new_serv.stmachine.Get(tokens[1])
				} else if (tokens[0]=="set"){
				        new_serv.stmachine.Set(tokens[1], tokens[2])
				        result = "Sucess"
				}
				//fmt.Println(result)
				new_serv.clioutchan <- result
				
			}
			select {

			case msg := <-new_serv.quit:
				if msg == true {
					if debug == 1 {
						fmt.Printf("Quitting server: %d\n", new_serv.id)
					}
					return
				}
			//If command received from client: append entry to local log
			case cmd := <-new_serv.outchan:
				logentry := LogItem{index, new_serv.Term(), cmd}
				index++
				new_serv.Log = append(new_serv.Log, logentry)
			case envelope := <-new_serv.server.Inbox():
				var rpctype, result string
				var msgterm, msgfrom, LastIndex, LastTerm int
				switch envelope.Msg.(type) {
				case string:
					message := envelope.Msg.(string)
					rpctype = strings.Split(message, " ")[0]
					msgfrom, _ = strconv.Atoi(strings.Split(message, " ")[2])
					msgterm, _ = strconv.Atoi(strings.Split(message, " ")[1])
				case map[string]interface{}:
					mes := envelope.Msg.(map[string]interface{})
					rpctype = mes["RpcType"].(string)
					result = mes["Result"].(string)
					msgfrom = int(mes["FromId"].(float64))
					msgterm = int(mes["LastTerm"].(float64))
					LastIndex = int(mes["LastIndex"].(float64))
					LastTerm = int(mes["LastTerm"].(float64))
				}
				//If RPC request or response contains term T > currentTerm:  set currentTerm = T, convert to follower
				if msgterm > new_serv.Term() {
					BecomeFollower(new_serv, msgterm, msgfrom)
				}
				if rpctype == "AppendEntriesResponse" {
					if result == "true" {
					        //fmt.Printf("Updated values for %d nextIndex - %d & matchIndex - %d\n",msgfrom, LastIndex,LastTerm)
						new_serv.nextIndex[msgfrom-1] = LastIndex
						new_serv.matchIndex[msgfrom-1] = LastTerm
					} else {
						if new_serv.nextIndex[msgfrom-1] > 1 {
							new_serv.nextIndex[msgfrom-1]--
						}
					}
				}
			default:
			        new_serv.SetTerm(new_serv.Term() + 1)
				//msg = "AppendEntries " + strconv.Itoa(new_serv.Term()) +" " + strconv.Itoa(new_serv.id) + " nil"
				var entries []LogItem
				rp := Rpc{"AppendEntries", new_serv.Term(), new_serv.id, -1, -1, -1, entries, "true"}
				new_serv.server.Outbox() <- &cluster.Envelope{Pid: cluster.BROADCAST, Msg: rp}
				timer2.Reset(tim2)
				if debug == 1 {
					fmt.Printf("Your leader server %d sends everyone an empty AppendEntries RPC\n", new_serv.id)
				}
				//If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
				if len(new_serv.Log) > 0 {
					for i := 1; i <= total; i++ {
						if (i != new_serv.id) && (len(new_serv.Log) >= new_serv.nextIndex[i-1]) {
						        //fmt.Printf("Length of Log: %d, Next Index: %d\n",len(new_serv.Log),new_serv.nextIndex[i-1])
						        //time.Sleep(75 * time.Millisecond)
							var entries []LogItem
							
							entries = new_serv.Log[new_serv.nextIndex[i-1]-1:]
							if new_serv.nextIndex[i-1] >= 2 {
							        //fmt.Println("a")
								rp := Rpc{"AppendEntries", new_serv.Term(), new_serv.id, new_serv.nextIndex[i-1]-1,
									new_serv.Log[new_serv.nextIndex[i-1]-2].Term, new_serv.commitIndex, entries, "true"}
								new_serv.server.Outbox() <- &cluster.Envelope{Pid: i, Msg: rp}
							} else {
							        //fmt.Println("b")
								rp := Rpc{"AppendEntries", new_serv.Term(), new_serv.id, 0,
									-1, new_serv.commitIndex, entries, "true"}
								new_serv.server.Outbox() <- &cluster.Envelope{Pid: i, Msg: rp}
							}
							//fmt.Println(entries)
						}
					}
				}
				time.Sleep(375 * time.Millisecond)

			}
			//fmt.Printf("Leader commitIndex - %d\n", new_serv.commitIndex)
			//If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
			ln := len(new_serv.Log)
			if new_serv.isLeader() == true {
				for i := new_serv.commitIndex + 1; i <= ln; i++ {
					cnt := 0
					for j := 0; j < total; j++ {
						if new_serv.matchIndex[j] >= i {
							cnt++
						}
					}
					//fmt.Printf("Count - %d, total - %d, log[N].term - %d, current term - %d\n", cnt, total,new_serv.Log[i-1].Term,new_serv.Term())
					if (cnt*2 >= total) && (new_serv.Log[i-1].Term <= new_serv.Term()) {
						new_serv.commitIndex++
						//fmt.Printf("commitIndex - %d, lastapplied - %d\n", new_serv.commitIndex,new_serv.lastApplied)
					} else {
						break
					}
				}
			}
		}
	}
}

func PrintState(b bool) {
	var state string
	if b == true {
		state = "leader"
	} else {
		state = "follower"
	}
	fmt.Printf("%s\n", state)
}
func BecomeFollower(new_serv *node, msgterm int, msgfrom int) {
	new_serv.SetTerm(msgterm)
	new_serv.SetVotedFor(-1)
	new_serv.SetVoteCount(0)
	new_serv.SetLeader(false)
	new_serv.leader = msgfrom
}
