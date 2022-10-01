package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"

	ebsfile "github.com/masahiro331/go-ebs-file"
)

func main() {
	var rs io.ReadSeeker
	var err error
	cli := ebsfile.New(ebsfile.Option{})
	rs, err = ebsfile.Open(os.Args[1], context.Background(), nil, cli)
	if err != nil {
		log.Fatal(err)
	}

	buf := make([]byte, 512)
	rs.Read(buf)
	fmt.Println(hex.Dump(buf))
	rs.Seek(0, io.SeekStart)

	rs.Read(buf)
	fmt.Println(hex.Dump(buf))
}