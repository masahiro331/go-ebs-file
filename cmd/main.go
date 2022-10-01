package main

import (
	"context"
	"encoding/hex"
	"fmt"
	ebsfile "github.com/masahiro331/go-ebs-file"
	"io"
	"log"
)

func main() {
	var rs io.ReadSeeker
	var err error
	cli := ebsfile.New(ebsfile.Option{})
	rs, err = ebsfile.Open("snap-03f84a11172ba6b40", context.Background(), nil, cli)
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
