package cluster

import (
	"context"
	"encoding/json"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/satori/go.uuid"
)

// Task is a stateful representation of work that should be performed
type Task struct {
	// ID is a unique identifier of the task
	ID string `json:"ID"`
	// Checkpoint contains enough information about the state of the
	// progress on a task that another worker can continue where the
	// previous left off.
	Checkpoint interface{} `json:"Checkpoint"`
	// Payload contains parameters used to start the task.
	Payload interface{} `json:"Payload"`
}

// WorkQueue is a way to receive work to be processed reliably.
type WorkQueue interface {
	Push(target string, payload interface{})
	Subscribe() <-chan Task
	Unsubscribe()
	CheckIn(task Task)
	Drop(task Task)
	Complete(task Task)
}

type MockedWorkQueue struct {
	busy  bool
	tasks chan Task
}

func (wq *MockedWorkQueue) Push(target string, payload interface{}) {
	if wq.tasks == nil {
		wq.tasks = make(chan Task, 128)
	}
	wq.tasks <- Task{Payload: payload}
}

func (wq *MockedWorkQueue) Subscribe() chan Task {
	return wq.tasks
}

func (wq *MockedWorkQueue) Unsubscribe() {
	close(wq.tasks)
	wq.tasks = nil
}

func (wq *MockedWorkQueue) CheckIn(task Task) {}

func (wq *MockedWorkQueue) Drop(task Task) {}

func (wq *MockedWorkQueue) Complete(task Task) {}

// EtcdWorkQueue is an etcd backed storage work to be processed
// It will continuously poll for tasks and add them to a "running" queue.
// Then it will start working on those tasks and delete them when done.
type EtcdWorkQueue struct {
	EtcdStorageBase
	Target   string
	Type     string
	busy     bool
	stopChan chan bool
}

func NewEtcdWorkQueue(c *clientv3.Client, target, workType string) *EtcdWorkQueue {
	s := &EtcdWorkQueue{Target: target, Type: workType}
	s.Client = c
	s.stopChan = make(chan bool)
	return s
}

func (wq *EtcdWorkQueue) Push(target string, payload interface{}) {
	id := uuid.NewV4().String()
	task := Task{ID: id, Payload: payload}
	wq.put(task, target)
}

// lock prevents any other consumer from subscribing on the same queue.
// The lock will be released when this node stops.
func (wq *EtcdWorkQueue) lock() error {
	session, err := concurrency.NewSession(wq.Client)
	if err != nil {
		return err
	}
	mtx := concurrency.NewMutex(session, wq.path("tasks/lock/"+wq.Target))
	return mtx.Lock(context.Background())
}

func (wq *EtcdWorkQueue) targetPath(target string) string {
	return wq.path("tasks/pending/" + wq.Type + "/" + target)
}

func (wq *EtcdWorkQueue) targetPathId(target, id string) string {
	return wq.targetPath(target) + "/" + id
}

// Subscribe listens for updates to the pending folder to emit new tasks.
// It is built on the assumption that only one client will be looking for tasks on the same queue.
func (wq *EtcdWorkQueue) Subscribe() <-chan Task {
	if wq.busy {
		panic("Only one subscriber per queue")
	}
	wq.busy = true
	wq.lock()

	tasks := make(chan Task, 1028)

	// Retrieve all existing tasks
	resp, err := wq.Client.Get(context.Background(), wq.targetPath(wq.Target), clientv3.WithPrefix())
	if err != nil {
		panic(err)
	}
	for _, kv := range resp.Kvs {
		tasks <- wq.unmarshal(kv.Value)
	}

	// When all are done, wait for new ones.
	watch := wq.Client.Watch(context.Background(), wq.targetPath(wq.Target), clientv3.WithPrefix())
	go func() {
		for {
			select {
			case resp := <-watch:
				for _, ev := range resp.Events {
					if ev.Type == mvccpb.PUT && ev.IsCreate() {
						tasks <- wq.unmarshal(ev.Kv.Value)
					}
				}
			case <-wq.stopChan:
				return
			}
		}
	}()
	return tasks
}

func (wq *EtcdWorkQueue) Clear() error {
	_, err := wq.Client.Delete(context.Background(), wq.path("tasks"), clientv3.WithPrefix())
	return err
}

func (wq *EtcdWorkQueue) unmarshal(data []byte) Task {
	var task Task
	err := json.Unmarshal(data, &task)
	if err != nil {
		panic(err)
	}
	return task
}

func (wq *EtcdWorkQueue) Unsubscribe() {
	close(wq.stopChan)
}

func (wq *EtcdWorkQueue) CheckIn(task Task) {
	wq.put(task, wq.Target)
}

func (wq *EtcdWorkQueue) Drop(task Task) {}

func (wq *EtcdWorkQueue) Complete(task Task) {
	wq.Client.Delete(context.Background(), wq.targetPathId(wq.Target, task.ID))
}

func (wq *EtcdWorkQueue) put(task Task, target string) {
	data, err := json.Marshal(task)
	if err != nil {
		panic(err)
	}
	_, err = wq.Client.Put(context.Background(), wq.targetPathId(target, task.ID), string(data))
	if err != nil {
		panic(err)
	}
}
