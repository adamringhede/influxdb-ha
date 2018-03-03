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

// Task data is used to deserialize tasks added to the queue
type TaskData struct {
	ID string `json:"ID"`
	Checkpoint json.RawMessage `json:"Checkpoint"`
	Payload json.RawMessage `json:"Payload"`
}

func (task *TaskData) Unmarshal(payload interface{}, checkpoint interface{}) (err error) {
	if task.Payload != nil {
		err = json.Unmarshal(task.Payload, &payload)
	}
	if err == nil && task.Checkpoint != nil {
		err = json.Unmarshal(task.Checkpoint, &checkpoint)
	}
	return err
}

type WorkSubscriber interface {
	Subscribe() <-chan TaskData
	Unsubscribe()
	CheckIn(task Task)
	Complete(task Task)
}

type WorkPublisher interface {
	Push(target string, payload interface{})
	Drop(task Task)
}

// WorkQueue is a way to receive work to be processed reliably.
type WorkQueue interface {
	WorkPublisher
	WorkSubscriber
}

type MockedWorkQueue struct {
	busy  bool
	tasks chan TaskData
}

func (wq *MockedWorkQueue) Push(target string, payload interface{}) {
	if wq.tasks == nil {
		wq.tasks = make(chan TaskData, 128)
	}
	task := Task{Payload: payload}
	taskRaw, _ := json.Marshal(task)
	var taskData TaskData
	json.Unmarshal(taskRaw, &taskData)
	wq.tasks <- taskData
}

func (wq *MockedWorkQueue) Subscribe() chan TaskData {
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
	mtx		*concurrency.Mutex
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
	err = mtx.Lock(context.Background())
	if err != nil {
		return err
	}
	wq.mtx = mtx
	return nil
}

func (wq *EtcdWorkQueue) unlock() {
	if wq.mtx != nil {
		wq.mtx.Unlock(context.Background())
	}
}

func (wq *EtcdWorkQueue) targetPath(target string) string {
	return wq.path("tasks/pending/" + wq.Type + "/" + target)
}

func (wq *EtcdWorkQueue) targetPathId(target, id string) string {
	return wq.targetPath(target) + "/" + id
}

// Subscribe listens for updates to the pending folder to emit new tasks.
// It is built on the assumption that only one client will be looking for tasks on the same queue.
func (wq *EtcdWorkQueue) Subscribe() <-chan TaskData {
	if wq.busy {
		panic("Only one subscriber per queue")
	}
	wq.busy = true
	wq.lock()

	tasks := make(chan TaskData, 1028)

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
				wq.unlock()
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

func (wq *EtcdWorkQueue) unmarshal(data []byte) TaskData {
	var task TaskData
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
