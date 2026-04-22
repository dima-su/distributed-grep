package storage

import (
	"context"
	"strings"
	"sync"

	"grep-distributed/pkg/gen"
)

type GrepServer struct {
	gen.UnimplementedDistributedGrepServer
	localText []string

	mutex sync.RWMutex
}

func (serv *GrepServer) AddText(req *gen.GrepAddRequest) (*gen.GrepAddResponse, error) {
	serv.mutex.Lock()
	defer serv.mutex.Unlock()

	serv.localText = append(serv.localText, req.NewText...)
	return &gen.GrepAddResponse{StatusCode: 0}, nil
}

func (serv *GrepServer) Grep(ctx context.Context, req *gen.GrepRequest) (*gen.GrepResponse, error) {
	serv.mutex.RLock()
	defer serv.mutex.RUnlock()

	var matches []string
	for _, Text := range serv.localText {
		if strings.Contains(req.Query, Text) {
			matches = append(matches, Text)
		}
	}
	return &gen.GrepResponse{Matches: matches, MatchCount: int32(len(matches))}, nil
}
