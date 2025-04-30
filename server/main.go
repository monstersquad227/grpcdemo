package main

import (
	"google.golang.org/grpc"
	pb "grpcdemo/proto"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
)

type server struct {
	pb.UnimplementedFlowEdgeServer
	streams sync.Map
}

func (s *server) Communicate(stream pb.FlowEdge_CommunicateServer) error {
	var agentID string
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			log.Printf("Agent %s disconnected", agentID)
			if agentID != "" {
				s.streams.Delete(agentID)
			}
			return nil
		}
		if err != nil {
			log.Printf("Recv error from agent %s: %v", agentID, err)
			if agentID != "" {
				s.streams.Delete(agentID)
			}
			return err
		}

		switch msg.Type {
		case pb.MessageType_REGISTER:
			log.Printf("Register: %+v", msg.GetRegister())
			agentID = msg.GetRegister().AgentId
			s.streams.Store(agentID, stream)
			log.Printf("Agent %s registered", agentID)
			// 回复执行指令
			//stream.Send(&pb.StreamMessage{
			//	Type: pb.MessageType_EXECUTE_REQUEST,
			//	Body: &pb.StreamMessage_ExecuteRequest{
			//		ExecuteRequest: &pb.ExecuteRequest{
			//			CommandId:    "cmd-001",
			//			ShellCommand: "ls",
			//		},
			//	},
			//})

		case pb.MessageType_HEARTBEAT:
			log.Printf("Heartbeat from %s", msg.GetHeartbeat().AgentId)

		case pb.MessageType_EXECUTE_RESPONSE:
			r := msg.GetExecuteResponse()
			log.Printf("Execution result: %s, output: %s, error: %s", r.CommandId, r.Output, r.Error)
		}
	}
}

func main() {
	Server := &server{}
	// 启动 HTTP 服务
	http.HandleFunc("/execute", func(w http.ResponseWriter, r *http.Request) {
		agentID := r.URL.Query().Get("agent_id")
		command := r.URL.Query().Get("command")
		//if agentID == "" || command == "" {
		//	http.Error(w, "agent_id and command required", http.StatusBadRequest)
		//	return
		//}
		streamVal, ok := Server.streams.Load(agentID)
		if !ok {
			http.Error(w, "agent not connected", http.StatusNotFound)
			return
		}
		stream := streamVal.(pb.FlowEdge_CommunicateServer)
		err := stream.Send(&pb.StreamMessage{
			Type: pb.MessageType_EXECUTE_REQUEST,
			Body: &pb.StreamMessage_ExecuteRequest{
				ExecuteRequest: &pb.ExecuteRequest{
					CommandId:    "cmd-http-001",
					ShellCommand: command,
				},
			},
		})
		if err != nil {
			http.Error(w, "failed to send command: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.Write([]byte("Command sent to agent " + agentID))
	})

	go func() {
		log.Println("HTTP server listening on :8081")
		log.Fatal(http.ListenAndServe(":8081", nil))
	}()

	// 启动 gRPC 服务器
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()

	pb.RegisterFlowEdgeServer(s, Server)
	log.Println("Server listening on :50051")
	err = s.Serve(lis)
	if err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
