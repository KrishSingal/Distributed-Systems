package paxos

import (
	"coms4113/hw5/pkg/base"
	"log"
)

const (
	Propose = "propose"
	Accept  = "accept"
	Decide  = "decide"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Proposer struct {
	N             int
	Phase         string
	N_a_max       int
	V             interface{}
	SuccessCount  int
	ResponseCount int
	// To indicate if response from peer is received, should be initialized as []bool of len(server.peers)
	Responses []bool
	// Use this field to check if a message is latest.
	SessionId int

	// in case node will propose again - restore initial value
	InitialValue interface{}
}

type ServerAttribute struct {
	peers []base.Address
	me    int

	// Paxos parameter
	n_p int
	n_a int
	v_a interface{}

	// final result
	agreedValue interface{}

	// Propose parameter
	proposer Proposer

	// retry
	timeout *TimeoutTimer
}

type Server struct {
	base.CoreNode
	ServerAttribute
}

func NewServer(peers []base.Address, me int, proposedValue interface{}) *Server {
	response := make([]bool, len(peers))
	return &Server{
		CoreNode: base.CoreNode{},
		ServerAttribute: ServerAttribute{
			peers: peers,
			me:    me,
			proposer: Proposer{
				InitialValue: proposedValue,
				Responses:    response,
			},
			timeout: &TimeoutTimer{},
		},
	}
}

func (server *Server) MessageHandler(message base.Message) []base.Node {

	newNodes := make([]base.Node, 0)

	// TODO: ACCEPTOR
	proposeReq, ok := message.(*ProposeRequest)
	if proposeReq != nil {
		// DPrintf("propose request %v\n", proposeReq)
	}
	if ok {
		newNode := server.copy()
		proposeResponse := ProposeResponse{
			CoreMessage: base.MakeCoreMessage(proposeReq.To(), proposeReq.From()),
			N_p:         newNode.n_p,
			N_a:         newNode.n_a,
			V_a:         newNode.v_a,
			SessionId:   proposeReq.SessionId,
		}

		if proposeReq.N > newNode.n_p {
			newNode.n_p = proposeReq.N

			proposeResponse.Ok = true
		} else {
			proposeResponse.Ok = false
		}

		// TODO: add message to server's queue
		messages := newNode.Response
		messages = append(messages, &proposeResponse)
		newNode.SetResponse(messages)

		newNodes = append(newNodes, newNode)
		// DPrintf("Added node %v", newNode)
	}

	acceptReq, ok := message.(*AcceptRequest)
	if acceptReq != nil {
		// DPrintf("accept request %v\n", acceptReq)
	}
	if ok {
		newNode := server.copy()
		acceptResponse := AcceptResponse{
			CoreMessage: base.MakeCoreMessage(acceptReq.To(), acceptReq.From()),
			N_p:         newNode.n_p,
			SessionId:   acceptReq.SessionId,
		}

		if acceptReq.N >= newNode.n_p {
			newNode.n_p = acceptReq.N
			newNode.n_a = acceptReq.N
			newNode.v_a = acceptReq.V

			DPrintf("Set v_a of %d to %v", newNode.me, newNode.v_a)

			acceptResponse.Ok = true
		} else {
			acceptResponse.Ok = false
		}

		// TODO: add message to server's queue
		messages := newNode.Response
		messages = append(messages, &acceptResponse)
		newNode.SetResponse(messages)

		newNodes = append(newNodes, newNode)
	}

	decideReq, ok := message.(*DecideRequest)
	if ok {
		newNode := server.copy()
		newNode.agreedValue = decideReq.V
		newNode.v_a = decideReq.V // TODO: Questionable...

		DPrintf("Set agreed value of %d to %v", newNode.me, newNode.agreedValue)

		newNodes = append(newNodes, newNode)
	}
	// TODO: END OF ACCEPTOR

	// TODO: PROPOSER
	proposeRes, ok := message.(*ProposeResponse)
	if proposeRes != nil {
		// DPrintf("proposal response is %v\n", proposeRes)
		// DPrintf("server looks like %v\n", server)
	}
	if ok && server.proposer.Phase == Propose && proposeRes.SessionId == server.proposer.SessionId {
		total := len(server.proposer.Responses)

		fromAddr := proposeRes.From()
		if !server.proposer.Responses[server.serverIndex(fromAddr)] { // non-duplicate, process it

			server.proposer.ResponseCount++
			server.proposer.Responses[server.serverIndex(fromAddr)] = true

			if proposeRes.Ok {
				server.proposer.SuccessCount++
				if proposeRes.N_a > server.proposer.N_a_max {
					server.proposer.N_a_max = proposeRes.N_a
					server.proposer.V = proposeRes.V_a
				}
			}

			if server.proposer.SuccessCount >= (total/2)+1 {
				// Case 1: Send out accept requests
				movingNode := server.copy()
				movingNode.proposer.Phase = Accept
				movingNode.proposer.ResponseCount = 0
				movingNode.proposer.SuccessCount = 0
				movingNode.proposer.Responses = make([]bool, len(movingNode.peers))
				myAddr := movingNode.peers[movingNode.me]

				for _, peer := range movingNode.peers {
					acceptRequest := AcceptRequest{
						CoreMessage: base.MakeCoreMessage(myAddr, peer),
						N:           movingNode.proposer.N,
						V:           movingNode.proposer.V,
						SessionId:   movingNode.proposer.SessionId,
					}

					// TODO: add accept request to message queue somehow
					messages := movingNode.Response
					messages = append(messages, &acceptRequest)
					movingNode.SetResponse(messages)
				}

				newNodes = append(newNodes, movingNode)

				// Case 2: Just wait
				if server.proposer.ResponseCount != total {
					staticNode := server.copy()
					newNodes = append(newNodes, staticNode)
				}
			} else {
				staticNode := server.copy()
				// DPrintf("static node added %v\n", staticNode)
				newNodes = append(newNodes, staticNode)
			}
		} else { // duplicate -- just give back current state
			staticNode := server.copy()
			// fmt.Printf("Duplicate response. static node added %v\n", staticNode)
			newNodes = append(newNodes, staticNode)
		}
	}
	if ok && server.proposer.Phase != Propose && proposeRes.SessionId == server.proposer.SessionId {
		movedOnNode := server.copy()
		newNodes = append(newNodes, movedOnNode)
	}
	if ok && proposeRes.SessionId != server.proposer.SessionId {
		wrongIdNode := server.copy()
		newNodes = append(newNodes, wrongIdNode)
	}

	// ACCEPT
	acceptRes, ok := message.(*AcceptResponse)
	if acceptRes != nil {
		// DPrintf("accept response is %v\n", acceptRes)
		// DPrintf("server looks like %v\n", server)
	}
	if ok && server.proposer.Phase == Accept && acceptRes.SessionId == server.proposer.SessionId {
		total := len(server.proposer.Responses)

		fromAddr := acceptRes.From()
		if !server.proposer.Responses[server.serverIndex(fromAddr)] {

			server.proposer.ResponseCount++
			server.proposer.Responses[server.serverIndex(fromAddr)] = true

			if acceptRes.Ok {
				server.proposer.SuccessCount++
			}

			if server.proposer.SuccessCount >= (total/2)+1 { // just reached majority
				// Case 1: Send out decide requests
				movingNode := server.copy()
				movingNode.proposer.Phase = Decide
				movingNode.proposer.ResponseCount = 0
				movingNode.proposer.SuccessCount = 0
				movingNode.proposer.Responses = make([]bool, len(movingNode.peers))
				// movingNode.agreedValue = movingNode.proposer.V // TODO: Double Check this
				myAddr := movingNode.peers[movingNode.me]

				for _, peer := range movingNode.peers {
					decideRequest := DecideRequest{
						CoreMessage: base.MakeCoreMessage(myAddr, peer),
						V:           movingNode.proposer.V,
						SessionId:   movingNode.proposer.SessionId,
					}

					// TODO: add decide request to message queue somehow
					messages := movingNode.Response
					messages = append(messages, &decideRequest)
					movingNode.SetResponse(messages)
				}

				newNodes = append(newNodes, movingNode)

				// Case 2: Just wait
				if server.proposer.ResponseCount != total {
					staticNode := server.copy()
					newNodes = append(newNodes, staticNode)
				}
			} else {
				staticNode := server.copy()
				// DPrintf("static node added %v\n", staticNode)
				newNodes = append(newNodes, staticNode)
			}
		} else {
			staticNode := server.copy()
			// DPrintf("static node added %v\n", staticNode)
			newNodes = append(newNodes, staticNode)
		}
	}
	if ok && server.proposer.Phase != Accept && acceptRes.SessionId == server.proposer.SessionId {
		movedOnNode := server.copy()
		newNodes = append(newNodes, movedOnNode)
	}
	if ok && acceptRes.SessionId != server.proposer.SessionId {
		wrongIdNode := server.copy()
		newNodes = append(newNodes, wrongIdNode)
	}
	// TODO: END OF PROPOSER

	/*if len(newNodes) != 1 {
		fmt.Printf("Returning %d nodes. Message is %v. server proposer is %v\n", len(newNodes), message, server.proposer)
		_, okay := message.(*ProposeResponse)
		fmt.Printf("Message is a propose response: %t\n", okay)
	}*/
	return newNodes
}

func (server *Server) serverIndex(address base.Address) int {
	for i, s := range server.peers {
		if s == address {
			return i
		}
	}

	return -1
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

// To start a new round of Paxos.
func (server *Server) StartPropose() {
	// TODO: implement it

	if server.proposer.InitialValue == nil {
		return
	}

	newN := server.n_p + 1
	newSession := server.proposer.SessionId + 1

	// don't make new proposer. Update old one

	server.proposer.N = newN
	server.proposer.Phase = Propose
	server.proposer.SuccessCount = 0
	server.proposer.ResponseCount = 0
	server.proposer.Responses = make([]bool, len(server.peers))
	server.proposer.SessionId = newSession

	server.proposer.N_a_max = 0
	server.proposer.V = server.proposer.InitialValue

	myAddr := server.peers[server.me]
	for _, peer := range server.peers {
		proposeRequest := ProposeRequest{
			CoreMessage: base.MakeCoreMessage(myAddr, peer),
			N:           newN,
			SessionId:   newSession,
		}

		// TODO: add propose request to message queue somehow
		messages := server.Response
		messages = append(messages, &proposeRequest)
		server.SetResponse(messages)
	}

	// panic("implement me")
}

// Returns a deep copy of server node
func (server *Server) copy() *Server {
	response := make([]bool, len(server.peers))
	for i, flag := range server.proposer.Responses {
		response[i] = flag
	}

	var copyServer Server
	copyServer.me = server.me
	// shallow copy is enough, assuming it won't change
	copyServer.peers = server.peers
	copyServer.n_a = server.n_a
	copyServer.n_p = server.n_p
	copyServer.v_a = server.v_a
	copyServer.agreedValue = server.agreedValue
	copyServer.proposer = Proposer{
		N:             server.proposer.N,
		Phase:         server.proposer.Phase,
		N_a_max:       server.proposer.N_a_max,
		V:             server.proposer.V,
		SuccessCount:  server.proposer.SuccessCount,
		ResponseCount: server.proposer.ResponseCount,
		Responses:     response,
		InitialValue:  server.proposer.InitialValue,
		SessionId:     server.proposer.SessionId,
	}

	// doesn't matter, timeout timer is state-less
	copyServer.timeout = server.timeout

	return &copyServer
}

func (server *Server) NextTimer() base.Timer {
	return server.timeout
}

// A TimeoutTimer tick simulates the situation where a proposal procedure times out.
// It will close the current Paxos round and start a new one if no consensus reached so far,
// i.e. the server after timer tick will reset and restart from the first phase if Paxos not decided.
// The timer will not be activated if an agreed value is set.
func (server *Server) TriggerTimer() []base.Node {
	if server.timeout == nil {
		return nil
	}

	subNode := server.copy()
	subNode.StartPropose()

	return []base.Node{subNode}
}

func (server *Server) Attribute() interface{} {
	return server.ServerAttribute
}

func (server *Server) Copy() base.Node {
	return server.copy()
}

func (server *Server) Hash() uint64 {
	return base.Hash("paxos", server.ServerAttribute)
}

func (server *Server) Equals(other base.Node) bool {
	otherServer, ok := other.(*Server)

	if !ok || server.me != otherServer.me ||
		server.n_p != otherServer.n_p || server.n_a != otherServer.n_a || server.v_a != otherServer.v_a ||
		(server.timeout == nil) != (otherServer.timeout == nil) {
		return false
	}

	if server.proposer.N != otherServer.proposer.N || server.proposer.V != otherServer.proposer.V ||
		server.proposer.N_a_max != otherServer.proposer.N_a_max || server.proposer.Phase != otherServer.proposer.Phase ||
		server.proposer.InitialValue != otherServer.proposer.InitialValue ||
		server.proposer.SuccessCount != otherServer.proposer.SuccessCount ||
		server.proposer.ResponseCount != otherServer.proposer.ResponseCount {
		return false
	}

	for i, response := range server.proposer.Responses {
		if response != otherServer.proposer.Responses[i] {
			return false
		}
	}

	return true
}

func (server *Server) Address() base.Address {
	return server.peers[server.me]
}
