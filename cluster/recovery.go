package cluster

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const nodeFileKeySep = "@"

type RecoveryChunk struct {
	DB, RP string
	Buf    []byte
}

type RecoveryStorage interface {
	// Put should save data so that it later can be sent to the data node when it recovers
	Put(nodeName, db, rp string, buf []byte) error
	// Get will return a channel of for streaming data for a certain node
	Get(nodeName string) (chan RecoveryChunk, error)
	// Remove data for a node
	Drop(nodeName string) error
}

type LocalRecoveryStorage struct {
	Path  string
	hints HintStorage
	// files contain file descriptors for writing data used for node recovery
	files map[string]*os.File
	mtx sync.Mutex
}

// NewLocalRecoveryStorage create a LocalRecoveryStorage for saving data at the specified path in different files.
// A hint storage can be provided in order to save where data is placed for recovery.
func NewLocalRecoveryStorage(path string, hs HintStorage) *LocalRecoveryStorage {
	return &LocalRecoveryStorage{path, hs, map[string]*os.File{}, sync.Mutex{}}
}

func (s *LocalRecoveryStorage) getFilePath(nodeName, db, rp string) string {
	return filepath.Join(s.Path, createFilename(nodeName, db, rp))
}

func (s *LocalRecoveryStorage) getOrCreateFile(nodeName, db, rp string) (*os.File, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	fp := s.getFilePath(nodeName, db, rp)
	if f, ok := s.files[nodeFileKey(nodeName, fp)]; ok {
		return f, nil
	}
	f, err := os.OpenFile(fp, os.O_WRONLY, os.ModeAppend)
	if _, ok := err.(*os.PathError); ok {
		log.Printf("Creatings recovery file at %s", fp)
		dir := filepath.Dir(fp)
		if dir != "." {
			err = os.MkdirAll(dir, os.ModePerm)
			if err != nil {
				return f, err
			}
		}
		f, err = os.Create(fp)
		f.WriteString(db + "\n")
		f.WriteString(rp + "\n")
	}
	if err != nil {
		return f, err
	}
	s.files[nodeFileKey(nodeName, fp)] = f
	return f, err
}

func nodeFileKey(nodeName, fp string) string {
	return nodeName + nodeFileKeySep + fp
}

func (s *LocalRecoveryStorage) Put(nodeName, db, rp string, buf []byte) error {
	f, err := s.getOrCreateFile(nodeName, db, rp)
	if err != nil {
		return err
	}
	_, err = f.Write(buf)
	if err != nil {
		return fmt.Errorf("failed to write recovery data: %s", err.Error())
	}
	err = f.Sync()
	if err != nil {
		return err
	}
	if s.hints != nil {
		err = s.hints.Put(nodeName, StatusWaiting)
		if err != nil {
			return fmt.Errorf("failed to put recovery hint: %s", err.Error())
		}
	}
	return nil
}

func (s *LocalRecoveryStorage) Drop(nodeName string) (err error) {
	matches, err := filepath.Glob(s.getFilePath(nodeName, "*", "*"))
	for key, file := range s.files {
		if strings.Split(key, nodeFileKeySep)[0] == nodeName {
			file.Close()
			delete(s.files, key)
		}
	}
	for _, filePath := range matches {
		err = os.Remove(filePath)
	}
	if s.hints != nil {
		s.hints.Done(nodeName)
	}
	return err
}

// Get returns chunks of data, each containing 500 points, in a channel
func (s *LocalRecoveryStorage) Get(nodeName string) (chan RecoveryChunk, error) {
	matches, err := filepath.Glob(s.getFilePath(nodeName, "*", "*"))
	if err != nil {
		return nil, err
	}
	if len(matches) == 0 {
		fmt.Println("Recovery warning: No files were found at " + s.getFilePath(nodeName, "*", "*"))
	}
	ch := make(chan RecoveryChunk)
	go func() {
		for _, filePath := range matches {
			f, err := os.Open(filePath)
			if err != nil {
				fmt.Println("Recovery warning: Failed to open file at " + filePath)
				continue
			}
			scanner := bufio.NewScanner(f)
			scanner.Scan()
			db := scanner.Text()
			scanner.Scan()
			rp := scanner.Text()

			buf := []byte{}
			i := 0
			for scanner.Scan() {
				data := scanner.Bytes()
				if len(string(data)) == 0 {
					// avoid empty lines
					continue
				}
				buf = append(buf, data...)
				i++
				if i >= 500 {
					ch <- RecoveryChunk{db, rp, buf}
					i = 0
					buf = []byte{}
				}
			}
			if len(buf) > 0 {
				ch <- RecoveryChunk{db, rp, buf}
			}
			f.Close()
		}
		close(ch)
	}()
	return ch, nil
}

func (s *LocalRecoveryStorage) Close() {
	for _, f := range s.files {
		f.Close()
	}
}

func createFilename(nodeName, db, rp string) string {
	return fmt.Sprintf("recovery_data.%s.%s.%s", nodeName, db, rp)
}

func RecoverNodes(hs *EtcdHintStorage, data RecoveryStorage, nodes NodeCollection) {
	client := &http.Client{Timeout: time.Second * 2}
	for {
		// TODO This can probably be replaced with an event handler of some sort that listens for nodes to start up.
		// This would remove the need of the sleep. The faster the node is recovered, the faster the health of the cluster can be recovered.
		for target := range hs.Local {
			log.Println("Found offline node, checking for signs of life...")
			if targetNode, ok := nodes.Get(target); ok {
				if isAlive(targetNode.DataLocation, client) {
					err := recoverNode(targetNode, data)
					if err == nil {
						log.Printf("Finished recovering node %s\n", target)
						data.Drop(target)
						hs.Done(target)
					} else {
						log.Println(err.Error())
					}
				}
			} else {
				log.Println("Error (recovery): The target node was not found in map of nodes")
			}
		}
		time.Sleep(time.Second * 2)
	}
}

func recoverNode(node Node, data RecoveryStorage) error {
	ch, err := data.Get(node.Name)
	if err != nil {
		return fmt.Errorf("failed to recover data for node %s when reading data: %s", node.Name, err.Error())
	}
	for chunk := range ch {
		resp, err := postData(node.DataLocation, chunk.DB, chunk.RP, chunk.Buf)
		if err != nil {
			return fmt.Errorf("failed to recover data for node %s. Got error: %s", node.Name, err.Error())
		}
		if resp.StatusCode > 204 {
			body, _ := ioutil.ReadAll(resp.Body)
			return fmt.Errorf("failed to recover data for node %s at %s. Received response code: %d and body %s", node.Name, node.DataLocation, resp.StatusCode, string(body))
		}
	}
	return nil
}

func isAlive(location string, client *http.Client) bool {
	resp, err := client.Get("http://" + location + "/ping")
	return err == nil && resp.StatusCode >= 200 && resp.StatusCode <= 299
}

func postData(location, db, rp string, buf []byte) (*http.Response, error) {
	req, err := http.NewRequest("POST", "http://"+location+"/write", bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}
	query := []string{
		"db=" + db,
		"rp=" + rp,
	}

	req.URL.RawQuery = strings.Join(query, "&")
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	//if auth != "" {
	//	req.Header.Set("Authorization", auth)
	//}
	client := http.Client{Timeout: 60 * time.Second}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	return resp, err
}
