package syncing

import (
	"log"

	"github.com/adamringhede/influxdb-ha/cluster"
)

const (
	ReliableImportWorkName = "import"
)

type RImporter interface {
	Import(tokens []int, resolver *cluster.Resolver, target string)
}

// TODO consider maybe including the measurements if we have this information to start with.
// We need a way to keep track of measurements imported that does not have a partition key.
// Or if they do not have a partition key, we will import them first or last according to the checkpoint
type ReliableImportPayload struct {
	Tokens []int `json:"Tokens"`
}

type ReliableImportCheckpoint struct {
	// TokenIndex is the last index processed
	TokenIndex int
}
type ReliableImportTask struct {
	ID string
	Checkpoint ReliableImportCheckpoint
	Payload ReliableImportCheckpoint
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
			var checkpoint ReliableImportCheckpoint
			var payload ReliableImportPayload
			task.Unmarshal(&payload, &checkpoint)
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
	log.Printf("Processing task: %s", taskID)

	task := cluster.Task{}
	task.ID = taskID
	task.Payload = payload
	for i, token := range payload.Tokens[lastIndex:] {
		// TODO Do not checkin after every single token.
		// Instead figure out the maximum size of each token, and checkin based on the amount
		// of data processed.
		// TODO importing for a single token is inefficent as each import has some overhead.
		// TODO Consider having the importer implement a mini batch iterator
		// TODO handle failure to import by trying again later. This depends on the error of course and should only try again later if the error is recoverable.

		// maybe instead of just using tokens, the importer also can take an option of a batch size which could be in terms of series or points imported.
		// also, it could be a good idea to distribute databases (not measurements as some queries need them to be one the same node)
		imp.importer.Import([]int{token}, imp.resolver, imp.target)
		task.Checkpoint = ReliableImportCheckpoint{TokenIndex: i+1}
		imp.wq.CheckIn(task)
	}
	imp.wq.Complete(task)
}
