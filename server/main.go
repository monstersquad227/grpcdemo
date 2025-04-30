package main

import (
	"io"
	"log"
	"net"

	"google.golang.org/grpc"
	pb "grpcdemo/proto"
)

type server struct {
	pb.UnimplementedFlowEdgeServer
}

func (s *server) Communicate(stream pb.FlowEdge_CommunicateServer) error {
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			log.Printf("Recv error: %v", err)
			return err
		}

		switch msg.Type {
		case pb.MessageType_REGISTER:
			log.Printf("Register: %+v", msg.GetRegister())
			// 回复执行指令
			stream.Send(&pb.StreamMessage{
				Type: pb.MessageType_EXECUTE_REQUEST,
				Body: &pb.StreamMessage_ExecuteRequest{
					ExecuteRequest: &pb.ExecuteRequest{
						CommandId:    "cmd-001",
						ShellCommand: "echo hello from server",
					},
				},
			})

		case pb.MessageType_HEARTBEAT:
			log.Printf("Heartbeat from %s", msg.GetHeartbeat().AgentId)

		case pb.MessageType_EXECUTE_RESPONSE:
			r := msg.GetExecuteResponse()
			log.Printf("Execution result: %s, output: %s, error: %s", r.CommandId, r.Output, r.Error)
		}
	}
}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterFlowEdgeServer(s, &server{})
	log.Println("Server listening on :50051")
	err = s.Serve(lis)
	if err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
