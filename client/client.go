package main

import (
	"context"
	"fmt"
	proto "grpc_file_streaming/proto"
	"io"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var client proto.StreamUploadClient

func main() {
	//connection to grpc server
	conn, err := grpc.NewClient("localhost:9000", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Error in client service : ", err)
	}

	client = proto.NewStreamUploadClient(conn)

	batchSize := 1024 * 1024 * 4 //4 MB at a time
	uploadStreamFile("./Vaibhav_Sharma-Resume.pdf", batchSize)
}

func uploadStreamFile(path string, batchSize int) {
	t := time.Now()
	file, err := os.Open(path)
	if err != nil {
		fmt.Println("Error in opening file - ", err)
		return
	}

	//parting file into batches
	buf := make([]byte, batchSize)
	batchNumber := 1
	stream, err := client.Upload(context.TODO())
	if err != nil {
		panic(err)
	}

	for {
		num, err := file.Read(buf)
		if err == io.EOF {
			fmt.Println("Uploading complete to server")
			break
		}
		if err != nil {
			fmt.Println("Error is ", err)
			return
		}
		chunk := buf[:num]

		if err := stream.Send(&proto.UploadRequest{FilePath: path, Chunk: chunk}); err != nil {
			fmt.Println("Error in sending file ", err)
			return
		}
		log.Printf("Sent - batch #%v - size - %v\n", batchNumber, len(chunk))
		batchNumber += 1
	}
	res, err := stream.CloseAndRecv()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(time.Since(t))
	log.Printf("Sent - %v bytes - %s\n", res.GetFileSize(), res.GetMessage())
}
