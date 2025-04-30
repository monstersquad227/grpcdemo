package main

import (
	"context"
	"io"
	"log"
	"os/exec"
	"time"

	"google.golang.org/grpc"
	pb "grpcdemo/proto"
)

func main() {
	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()

	client := pb.NewFlowEdgeClient(conn)
	stream, err := client.Communicate(context.Background())
	if err != nil {
		log.Fatalf("stream error: %v", err)
	}

	// 注册信息
	stream.Send(&pb.StreamMessage{
		Type: pb.MessageType_REGISTER,
		Body: &pb.StreamMessage_Register{
			Register: &pb.RegisterMessage{
				AgentId:  "agent-001",
				Hostname: "localhost",
				Version:  "v1.0.0",
			},
		},
	})

	// 心跳 goroutine
	go func() {
		for {
			time.Sleep(5 * time.Second)
			stream.Send(&pb.StreamMessage{
				Type: pb.MessageType_HEARTBEAT,
				Body: &pb.StreamMessage_Heartbeat{
					Heartbeat: &pb.HeartbeatMessage{
						AgentId:   "agent-001",
						Timestamp: time.Now().Unix(),
					},
				},
			})
		}
	}()

	// 接收指令并执行
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("recv error: %v", err)
		}

		if msg.Type == pb.MessageType_EXECUTE_REQUEST {
			req := msg.GetExecuteRequest()
			log.Printf("Executing: %s", req.ShellCommand)
			cmd := exec.Command("sh", "-c", req.ShellCommand)
			output, err := cmd.CombinedOutput()
			errStr := ""
			if err != nil {
				errStr = err.Error()
			}
			stream.Send(&pb.StreamMessage{
				Type: pb.MessageType_EXECUTE_RESPONSE,
				Body: &pb.StreamMessage_ExecuteResponse{
					ExecuteResponse: &pb.ExecuteResponse{
						CommandId: req.CommandId,
						ExitCode:  0,
						Output:    string(output),
						Error:     errStr,
					},
				},
			})
		}
	}
}
