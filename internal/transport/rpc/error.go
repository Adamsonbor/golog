package rpc

import (
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ErrOffsetOutOfRange struct {
	Offset uint64
}

func (self ErrOffsetOutOfRange) GRPCStatus() *status.Status {
	st := status.New(
		codes.OutOfRange,
		fmt.Sprintf("offset out of range: %d", self.Offset),
	)

	msg := fmt.Sprintf("The request offset is outside the log's range.")
	d := &errdetails.LocalizedMessage{
		Locale: "en-US",
		Message: msg,
	}

	std, err := st.WithDetails(d)
	if err != nil {
		return st
	}

	return std
}

func (self ErrOffsetOutOfRange) Error() string {
	return self.GRPCStatus().Err().Error()
}
