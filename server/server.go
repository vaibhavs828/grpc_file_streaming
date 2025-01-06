package main

import (
	"fmt"
	proto "grpc_file_streaming/proto"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type server struct {
	proto.UnimplementedStreamUploadServer
}

func main() {
	listener, err := net.Listen("tcp", ":9000")
	if err != nil {
		log.Fatalf("Unable to start server : ", err)
	}
	grpcServer := grpc.NewServer()
	proto.RegisterStreamUploadServer(grpcServer, &server{})
	reflection.Register(grpcServer)

	if e := grpcServer.Serve(listener); e != nil {
		panic(e)
	}
}

// Server function to be invoked by client
func (s server) Upload(stream proto.StreamUpload_UploadServer) error {
	var fileBytes []byte
	var fileSize int64 = 0
	var fileName string = "MAC-"

	timeStamp := time.Now().Unix()
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			fileName += req.GetFilePath()
			fmt.Println("fileName rec is ", fileName)
			fileName += strconv.FormatInt(timeStamp, 10)
			log.Print("END OF FILE")
			break
		}
		chunks := req.GetChunk()
		fileBytes = append(fileBytes, chunks...) //to expand chunks to be appended into the file
		fileSize += int64(len(chunks))
	}

	f, err := os.Create(fmt.Sprintf("./%s.pdf", fileName))
	if err != nil {
		fmt.Println("GOT ERROR - ", err)
		return err
	}

	defer f.Close()
	_, writeErr := f.Write(fileBytes)
	if writeErr != nil {
		fmt.Println("WRITE ERROR - ", writeErr)
	}

	return stream.SendAndClose(&proto.UploadResponse{FileSize: fileSize, Message: "File written successfully"})

}
