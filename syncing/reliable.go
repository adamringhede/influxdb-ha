package syncing

import (
	"fmt"
	"log"

	"github.com/adamringhede/influxdb-ha/cluster"
)

const (
	ReliableImportWorkName = "import"
)

// TODO consider maybe including the Measurements if we have this information to start with.
// We need a way to keep track of Measurements imported that does not have a partition key.
// Or if they do not have a partition key, we will import them first or last according to the checkpoint
type ReliableImportPayload struct {
	Tokens         []int `json:"Tokens"`
	NonPartitioned bool
	AssignToSelf   bool
}

type ReliableImportCheckpoint struct {
	// TokenIndex is the last index processed
	TokenIndex     int
	NonPartitioned bool
}

type ReliableImporter struct {
	importer     Importer
	wq           cluster.WorkQueue
	resolver     *cluster.Resolver
	target       *InfluxClient
	stopChan     chan bool
	tokenStorage cluster.TokenStorage
	AfterImport  func(token int)
}

func NewReliableImporter(importer Importer, wq cluster.WorkQueue, resolver *cluster.Resolver, target *InfluxClient) *ReliableImporter {
	return &ReliableImporter{importer: importer, wq: wq, resolver: resolver, target: target, stopChan: make(chan bool)}
}

func (imp *ReliableImporter) Start() {
	tasks := imp.wq.Subscribe()
	for {
		select {
		case task := <-tasks:
			var checkpoint ReliableImportCheckpoint
			var payload ReliableImportPayload
			err := task.Unmarshal(&payload, &checkpoint)
			if err != nil {
				fmt.Printf("Failed to unmarshal task data. Payload %s, Checkoint: %s",
					string(task.Payload), string(task.Checkpoint))
			}
			imp.process(task.ID, payload, checkpoint)
		case <-imp.stopChan:
			return
		}
	}
}

func (imp *ReliableImporter) Stop() {
	imp.wq.Unsubscribe()
	close(imp.stopChan)
}

func (imp *ReliableImporter) process(taskID string, payload ReliableImportPayload, checkpoint ReliableImportCheckpoint) {
	lastIndex := checkpoint.TokenIndex
	log.Printf("Processing task: ImportPartitioned (%s)", taskID)

	task := cluster.Task{}
	task.ID = taskID
	task.Payload = payload

	if payload.NonPartitioned && !checkpoint.NonPartitioned {
		imp.importer.ImportNonPartitioned(imp.target)

		checkpoint.NonPartitioned = true
		task.Checkpoint = checkpoint
		imp.wq.CheckIn(task)
	}

	for i, token := range payload.Tokens[lastIndex:] {
		// TODO Do not checkin after every single token.
		// Instead figure out the maximum size of each token, and checkin based on the amount
		// of data processed.
		// TODO importing for a single token is inefficent as each import has some overhead.
		// TODO Consider having the importer implement a mini batch iterator
		// TODO handle failure to import by trying again later. This depends on the error of course and should only try again later if the error is recoverable.

		// maybe instead of just using tokens, the importer also can take an option of a batch size which could be in terms of series or points imported.
		// also, it could be a good idea to distribute databases (not Measurements as some queries need them to be one the same node)
		imp.importer.ImportPartitioned([]int{token}, imp.target)
		checkpoint.TokenIndex = i + 1
		task.Checkpoint = checkpoint

		if imp.AfterImport != nil {
			imp.AfterImport(token)
		}

		imp.wq.CheckIn(task)
	}
	imp.wq.Complete(task)
}
