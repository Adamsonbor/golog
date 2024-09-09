package service

import "logger/internal/domain"

type Log interface {
	Append(domain.Record) (uint64, error)
	Read(uint64) (domain.Record, error)
}

type Index interface {
	Read(int64) (uint32, uint64, error)
	Write(uint32, uint64) error
	Close() error
}

type FileStorage interface {
	Append([]byte) (uint64, uint64, error)
	Read(uint64) ([]byte, error)
}
