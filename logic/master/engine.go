package master

import (
	"context"
	"log"
	"sync"
	"time"

	gen "grep-distributed/api"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type GrepClient struct {
	workerAddresses  map[string]struct{}
	connectedWorkers map[string]*grpc.ClientConn
	mutx             sync.RWMutex
}

func (client *GrepClient) AddWorkers(workerAddresses ...string) error {
	client.mutx.Lock()
	defer client.mutx.Unlock()

	for _, workerAddress := range workerAddresses {
		client.workerAddresses[workerAddress] = struct{}{}
		log.Printf("Added worker %v", workerAddress)
	}
	return nil
}

func (client *GrepClient) DeleteWorkers(workerAddresses ...string) error {
	client.mutx.Lock()
	defer client.mutx.Unlock()

	for _, workerAddress := range workerAddresses {
		delete(client.workerAddresses, workerAddress)
		log.Printf("Deleted worker %v", workerAddress)
	}
	return nil
}

func (client *GrepClient) DisconnectWorkers() error {
	client.mutx.Lock()
	defer client.mutx.Unlock()

	for worker := range client.connectedWorkers {
		log.Printf("Worker %v is disconnected", worker)
		err := client.connectedWorkers[worker].Close()
		if err != nil {
			log.Printf("Worker %v returned err %v", worker, err)
		}
		delete(client.connectedWorkers, worker)
	}
	return nil
}

func (client *GrepClient) ConnectWorkers() error {
	client.mutx.Lock()
	defer client.mutx.Unlock()

	for address := range client.workerAddresses {
		if _, connected := client.connectedWorkers[address]; connected {
			continue
		}
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

func (client *GrepClient) CallAllWorkers(ctx context.Context, query string) ([]string, error) {
	client.mutx.RLock()
	defer client.mutx.RUnlock()

	resultChan := make(chan *gen.GrepResponse, len(client.connectedWorkers))
	var wg sync.WaitGroup

	log.Println("Calling all workers. . .")

	for addr, conn := range client.connectedWorkers {
		wg.Add(1)
		reciever := gen.NewDistributedGrepClient(conn)

		go func(worker gen.DistributedGrepClient, address string) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			oneAnsw, err := reciever.Grep(ctx, &gen.GrepRequest{Query: query})
			if err == nil {
				resultChan <- oneAnsw
			} else {
				log.Printf("Worker %v couldn't grep. Error code: %v", address, err)
			}
		}(reciever, addr)
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
