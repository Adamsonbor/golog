package auth

import (
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ErrPermissionDenied struct {
	subject string
	object  string
	action  string
}

func (self ErrPermissionDenied) Status() *status.Status {
	st := status.New(
		codes.PermissionDenied,
		fmt.Sprintf("%s not permitted to %s %s", self.subject, self.action, self.object),
	)

	return st
}

func (self ErrPermissionDenied) Error() string {
	return self.Status().Err().Error()
}
