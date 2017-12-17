package syncing

import (
	"log"

	"github.com/adamringhede/influxdb-ha/cluster"
	"github.com/mitchellh/mapstructure"
)

type RImporter interface {
	Import(tokens []int, resolver *cluster.Resolver, target string)
}

type ReliableImportPayload struct {
	Tokens []int `json:"Tokens"`
}

type ReliableImportCheckpoint struct {
	// TokenIndex is the last index processed
	TokenIndex int
}

type ReliableImporter struct {
	importer RImporter
	wq       cluster.WorkQueue
	resolver *cluster.Resolver
	target   string
	stopChan chan bool
}

func NewReliableImporter(importer RImporter, wq cluster.WorkQueue, resolver *cluster.Resolver, target string) *ReliableImporter {
	return &ReliableImporter{importer, wq, resolver, target, make(chan bool)}
}

func (imp *ReliableImporter) Start() {
	tasks := imp.wq.Subscribe()
	for {
		select {
		case task := <-tasks:
			imp.process(task)
		case <-imp.stopChan:
			return
		}
	}
}

func (imp *ReliableImporter) Stop() {
	imp.wq.Unsubscribe()
	close(imp.stopChan)
}

func reconstructPayload(data interface{}) ReliableImportPayload {
	var p ReliableImportPayload
	mapstructure.Decode(data, &p)
	return p
}

func reconstructCheckpoint(data interface{}) ReliableImportCheckpoint {
	var c ReliableImportCheckpoint
	mapstructure.Decode(data, &c)
	return c
}

func (imp *ReliableImporter) process(task cluster.Task) {
	lastIndex := -1
	log.Printf("Processing task: %s", task.ID)

	if task.Checkpoint != nil {
		checkpoint := reconstructCheckpoint(task.Checkpoint)
		lastIndex = checkpoint.TokenIndex
	}

	payload := reconstructPayload(task.Payload)
	for i, token := range payload.Tokens[lastIndex+1:] {
		// TODO Do not checkin after every single token.
		// Instead figure out the maximum size of each token, and checkin based on the amount
		// of data processed.
		imp.importer.Import([]int{token}, imp.resolver, imp.target)
		task.Checkpoint = ReliableImportCheckpoint{TokenIndex: i}
		imp.wq.CheckIn(task)
	}
	imp.wq.Complete(task)
}
