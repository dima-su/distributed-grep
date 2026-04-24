package master

import (
	"context"
	"log"
	"slices"
	"sync"
	"time"

	gen "grep-distributed/api"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type GrepClient struct {
	workerAdresses   []string
	connectedWorkers map[string]*grpc.ClientConn
}

func (client *GrepClient) AddWorkers(worker_adresses ...string) error {
	for _, worker_adress := range worker_adresses {
		for _, worker := range client.workerAdresses {
			if worker == worker_adress {
				log.Printf("Worker %v is already known.", worker_adress)
				break
			}
		}
		client.workerAdresses = append(client.workerAdresses, worker_adress)
		log.Printf("Added worker %v", worker_adress)
	}
	return nil
}

func (client *GrepClient) DeleteWorkers(worker_adresses ...string) error {
	for _, worker_adress := range worker_adresses {
		for i, adress := range client.workerAdresses {
			if adress == worker_adress {
				client.workerAdresses = slices.Delete(client.workerAdresses, i, i+1)
				log.Printf("Deleted worker %v", worker_adress)
				break
			}
		}
	}

	return nil
}

func (client *GrepClient) ConnectWorkers() error {
	client.connectedWorkers = make(map[string]*grpc.ClientConn)
	for _, address := range client.workerAdresses {
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("grpc couldn't establish connection with %v. error code: %v", address, err)
			return err
		} else {
			client.connectedWorkers[address] = conn
			log.Printf("gRPC established connection with worker on address %v", address)
		}
	}
	return nil
}

func (client *GrepClient) CallAllWorkers(query string) ([]string, error) {
	resultChan := make(chan *gen.GrepResponse, len(client.workerAdresses))
	var wg sync.WaitGroup

	log.Println("Calling all workers. . .")

	for _, conn := range client.connectedWorkers {
		wg.Add(1)
		reciever := gen.NewDistributedGrepClient(conn)

		go func(worker gen.DistributedGrepClient) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			oneAnsw, err := reciever.Grep(ctx, &gen.GrepRequest{Query: query})
			if err == nil {
				resultChan <- oneAnsw
			} else {
				log.Printf("Worker %v couldn't grep. Error code: %v", conn, err)
			}
		}(reciever)
	}
	go func() {
		log.Println("Done calling workers, waiting. . .")
		wg.Wait()
		close(resultChan)
		log.Println("Done waiting")
	}()
	log.Println("Got results from workers, summing up. . .")
	var result []string
	for res := range resultChan {
		result = append(result, res.Matches...)
	}
	log.Println("Calling workers done")
	return result, nil
}
