package paxos

import (
	"coms4113/hw5/pkg/base"
	"fmt"
)

// Fill in the function to lead the program to a state where A2 rejects the Accept Request of P1
func ToA2RejectP1() []func(s *base.State) bool {
	p1PreparePhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		// fmt.Printf("Got to p1PreparePhase\n")
		return s1.proposer.Phase == Propose
	}

	p1AcceptPhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		// fmt.Printf("Got to p1AcceptPhase\n")
		return s1.proposer.Phase == Accept
	}

	p3PreparePhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		// fmt.Printf("Got to p3PreparePhase\n")
		return s3.proposer.Phase == Propose
	}

	p3AcceptPhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		// fmt.Printf("Got to p3AcceptPhase\n")
		return s3.proposer.Phase == Accept
	}

	/*p1OneAcceptance := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		// fmt.Printf("Got to p1OneAcceptance\n")
		return s1.proposer.SuccessCount == 1
	}*/

	p1RejectedByp2 := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		valid := false
		rejectionFromp2 := false
		if s1.proposer.Phase == Accept && s1.proposer.V == "v1" {
			for _, m := range s.Network {
				resp, ok := m.(*AcceptResponse)
				if ok && !resp.Ok && m.To() == s1.Address() && m.From() == s2.Address() {
					rejectionFromp2 = true
				}
			}
			if rejectionFromp2 {
				valid = true
			}
		}
		if valid {
			fmt.Println("... p1's accept request got rejected by p2")
		}
		return valid
	}

	return []func(s *base.State) bool{p1PreparePhase, p1AcceptPhase, p3PreparePhase,
		p3AcceptPhase, p1RejectedByp2}
	// panic("fill me in")
}

// Fill in the function to lead the program to a state where a consensus is reached in Server 3.
func ToConsensusCase5() []func(s *base.State) bool {
	/*p3TwoAcceptance := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		// fmt.Printf("Got to p3TwoAcceptance\n")
		return s3.proposer.Phase == Accept && s3.proposer.SuccessCount == 2
	}*/

	/*p3Decide := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		// fmt.Printf("Got to p3Decide\n")
		return s3.proposer.Phase == Decide
	}*/

	s3KnowConsensus := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		// fmt.Printf("Got to s3KnowConsensus. Agreed value is %v\n", s3.agreedValue)
		return s3.agreedValue == "v3"
	}

	return []func(s *base.State) bool{s3KnowConsensus}
	// panic("fill me in")
}

// Fill in the function to lead the program to a state where all the Accept Requests of P1 are rejected
func NotTerminate1() []func(s *base.State) bool {
	p1PreparePhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		valid := s1.proposer.Phase == Propose && s1.proposer.N == 1
		if valid {
			// fmt.Printf("p1 has %d responses\n", s1.proposer.ResponseCount)
			// fmt.Printf("Got to p1PreparePhase\n")
		}
		return valid
	}

	/*npIsOne := func(s *base.State) bool {
		// valid := true
		// fmt.Printf("Checking n_p\n")
		count := 3
		for _, node := range s.Nodes() {
			server := node.(*Server)
			if server.n_p != 1 {
				// fmt.Printf("Server %v, has n_p = %d\n", server, server.n_p)
				count--
			}
		}

		return count >= 2
	}*/

	p1AcceptPhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		valid := s1.proposer.Phase == Accept && s1.proposer.N == 1 && len(s1.Response) == 0
		if valid {
			/*for _, node := range s.Nodes() {
				server := node.(*Server)
				// fmt.Printf("Server %v, has n_p = %d\n", server, server.n_p)
			}*/

			// fmt.Printf("p1 has responses %d\n", s1.proposer.ResponseCount)
			// fmt.Printf("Got to p1AcceptPhase\n")
		}
		return valid
	}

	/*p1AllProposeResponses := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)

		valid := s1.proposer.Phase == Propose && s1.proposer.SuccessCount == 3 && s1.proposer.N == 1
		if valid {
			// fmt.Printf("Got all p1 prepare responses\n")
		}
		return valid
	}*/

	p3PreparePhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		s1 := s.Nodes()["s1"].(*Server)

		valid := s3.proposer.Phase == Propose && s3.proposer.N == 2 && s1.proposer.Phase == Accept && s1.proposer.N == 1
		valid = valid && s1.proposer.ResponseCount == 0 && s1.proposer.SessionId == 1 && len(s1.Response) == 0
		if valid {
			/*for _, node := range s.Nodes() {
				// server := node.(*Server)
				// fmt.Printf("Server %v, has n_p = %d\n", server, server.n_p)
			}*/

			// fmt.Printf("p1 has responses %d\n", s1.proposer.ResponseCount)
			// fmt.Printf("Got to p3PreparePhase\n")
		}
		return valid
	}

	seeMessages := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		if s1.proposer.Phase == Accept && s1.proposer.V == "v1" {
			for _, m := range s.Network {
				_, ok := m.(*AcceptRequest)
				if ok && m.From() == s1.Address() {
					// fmt.Printf("%v\n", m)
				}
			}
		}
		return true
	}

	/*npIsTwo := func(s *base.State) bool {
		count := 3
		for _, node := range s.Nodes() {
			server := node.(*Server)
			if server.n_p != 2 {
				count--
			}
		}

		if count >= 2 {
			for _, node := range s.Nodes() {
				server := node.(*Server)
				// fmt.Printf("Server %v, has n_p = %d\n", server, server.n_p)
			}

			// fmt.Printf("NpIsTwo\n")
		}

		return count >= 2
	}*/

	/*noP1Messages := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		valid := true
		if s1.proposer.Phase == Accept && s1.proposer.V == "v1" {
			for _, m := range s.Network {
				_, ok := m.(*AcceptRequest)
				if ok && m.From() == s1.Address() {
					valid = false
				}
			}
		}
		if valid {
			fmt.Println("p1 hasn't sent anything out")
		}
		return valid
	}*/

	p3AcceptPhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		s1 := s.Nodes()["s1"].(*Server)

		valid := s3.proposer.Phase == Accept && s3.proposer.N == 2 && s3.proposer.ResponseCount == 0
		valid = valid && s1.proposer.Phase == Accept && s1.proposer.N == 1 && s1.proposer.SessionId == 1 && len(s1.Response) == 0 && s1.proposer.ResponseCount == 0
		// valid := s3.proposer.Phase == Propose && s3.proposer.N == 2 && s1.proposer.N == 1 && s1.proposer.Phase == Accept
		if valid {
			/*for _, node := range s.Nodes() {
				server := node.(*Server)
				// fmt.Printf("Server %v, has n_p = %d\n", server, server.n_p)
			}*/

			// fmt.Printf("Got to p3AcceptPhase\n")
		}
		return valid
	}

	/*p3AllProposeResponses := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		s1 := s.Nodes()["s1"].(*Server)

		valid := s3.proposer.Phase == Propose && s3.proposer.SuccessCount == 3 && s3.proposer.N == 2 && s1.proposer.N == 1 && s1.proposer.Phase == Accept
		if valid {
			// fmt.Printf("Got all p3 prepare responses\n")
		}
		return valid
	}*/

	/*p1AcceptPhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		valid := s1.proposer.Phase == Accept && s1.proposer.N == 1
		if valid {
			// fmt.Printf("Got to p1AcceptPhase\n")
		}
		return valid
	}*/

	/*p1AllAcceptRejects := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		// fmt.Printf("Got all prepare responses\n")
		return s1.proposer.Phase == Accept && s1.proposer.ResponseCount == 3 && s1.proposer.SuccessCount == 0
	}*/

	return []func(s *base.State) bool{p1PreparePhase, p1AcceptPhase, p3PreparePhase, seeMessages, p3AcceptPhase}
	// panic("fill me in")
}

// Fill in the function to lead the program to a state where all the Accept Requests of P3 are rejected
func NotTerminate2() []func(s *base.State) bool {
	p1PreparePhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		// fmt.Printf("(2) Got to p1PreparePhase\n")
		return s1.proposer.Phase == Propose
	}

	p1AllProposeResponses := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		// fmt.Printf("(2) Got all p1 prepare responses\n")
		return s1.proposer.Phase == Propose && s1.proposer.SuccessCount == 3
	}

	return []func(s *base.State) bool{p1PreparePhase, p1AllProposeResponses}
	// panic("fill me in")
}

// Fill in the function to lead the program to a state where all the Accept Requests of P1 are rejected again.
func NotTerminate3() []func(s *base.State) bool {
	p3PreparePhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		// fmt.Printf("(2) Got to p3PreparePhase\n")
		return s3.proposer.Phase == Propose
	}

	p3AllProposeResponses := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		// fmt.Printf("(2) Got all p3 prepare responses\n")
		return s3.proposer.Phase == Propose && s3.proposer.SuccessCount == 3
	}

	return []func(s *base.State) bool{p3PreparePhase, p3AllProposeResponses}
	/// panic("fill me in")
}

// Fill in the function to lead the program to make P1 propose first, then P3 proposes, but P1 get rejects in
// Accept phase
func concurrentProposer1() []func(s *base.State) bool {
	panic("fill me in")
}

// Fill in the function to lead the program continue  P3's proposal  and reaches consensus at the value of "v3".
func concurrentProposer2() []func(s *base.State) bool {
	panic("fill me in")
}
