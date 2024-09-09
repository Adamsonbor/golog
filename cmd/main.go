package main

import (
	"log"
	v1 "logger/gen/go/v1"
	"logger/internal/service/config"
	logger "logger/internal/service/log"
)

func main() {
	l, err := logger.New("./logs", &config.Config{
		Segment: config.Segment{
			MaxStoreBytes: 1024,
			MaxIndexBytes: 1024,
			InitialOffset: 0,
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Println(l.Dir)

	defer l.Close()

	log.Println("Appending record")
	off, err := l.Append(&v1.Record{
		Value: []byte(`{"foo": "bar"}`),
	})
	if err != nil {
		log.Fatal("Failed to append record: ", err)
	}

	log.Println("Record appended at offset: ", off)

	record, err := l.Read(off)
	if err != nil {
		log.Fatal("Failed to read record: ", err)
	}

	log.Printf("Record: %v\n", record)
	log.Println("Done")
}
