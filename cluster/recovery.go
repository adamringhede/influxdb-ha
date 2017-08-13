package cluster

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"time"
	"net/http"
	"net/url"
	"bytes"
	"strconv"
	"strings"
)

type RecoveryChunk struct {
	DB, RP string
	Buf []byte
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
}

// NewLocalRecoveryStorage create a LocalRecoveryStorage for saving data at the specified path in different files.
// A hint storage can be provided in order to save where data is placed for recovery.
func NewLocalRecoveryStorage(path string, hs HintStorage) *LocalRecoveryStorage {
	return &LocalRecoveryStorage{path, hs, map[string]*os.File{}}
}

func (s *LocalRecoveryStorage) getFilePath(nodeName, db, rp string) string {
	return filepath.Join(s.Path, createFilename(nodeName, db, rp))
}

func (s *LocalRecoveryStorage) getOrCreateFile(nodeName, db, rp string) (*os.File, error) {
	filePath := s.getFilePath(nodeName, db, rp)
	if f, ok := s.files[filePath]; ok {
		return f, nil
	}
	f, err := os.OpenFile(filePath, os.O_WRONLY, os.ModeAppend)
	if _, ok := err.(*os.PathError); ok {
		f, err = os.Create(filePath)
		f.WriteString(db + "\n")
		f.WriteString(rp + "\n")
	}
	if err != nil {
		return f, err
	}
	s.files[filePath] = f
	return f, err
}

func (s *LocalRecoveryStorage) Put(nodeName, db, rp string, buf []byte) error {
	f, err := s.getOrCreateFile(nodeName, db, rp)
	if err != nil {
		return err
	}
	_, err = f.Write(append(buf, []byte("\n")...))
	if err != nil {
		return fmt.Errorf("Failed to write recovery data: %s", err.Error())
	}
	err = f.Sync()
	if err != nil {
		return err
	}
	if s.hints != nil {
		err = s.hints.Put(nodeName, StatusWaiting)
	}
	if err != nil {
		return fmt.Errorf("Failed to put recovery hint: %s", err.Error())
	}
	return nil
}

func (s *LocalRecoveryStorage) Drop(nodeName string) (err error) {
	matches, err := filepath.Glob(s.getFilePath(nodeName, "*", "*"))
	for _, filePath := range matches {
		err = os.Remove(filePath)
	}
	return err
}

func (s *LocalRecoveryStorage) Get(nodeName string) (chan RecoveryChunk, error) {
	matches, err := filepath.Glob(s.getFilePath(nodeName, "*", "*"))
	if err != nil {
		return nil, err
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
	return fmt.Sprintf("recovery_data_%s_%s_%s", nodeName, db, rp)
}

func RecoverNodes(hs *EtcdHintStorage, data RecoveryStorage, nodes map[string]*Node) {
	client := &http.Client{Timeout:time.Second * 2}
	for {
		for target := range hs.Local {
			if targetNode, ok := nodes[target]; ok {
				if isAlive(targetNode.DataLocation, client) {
					err := recoverNode(targetNode, data)
					if err == nil {
						fmt.Printf("Finished recovering node %s\n", target)
						data.Drop(target)
					} else {
						fmt.Println(err.Error())
					}
				}
			}
		}
		time.Sleep(time.Second)
	}
}

func recoverNode(node *Node, data RecoveryStorage) error {
	ch, err := data.Get(node.Name)
	if err != nil {
		return fmt.Errorf("Failed to recover data for node %s when reading data: %s", node.Name, err.Error())
	}
	for chunk := range ch {
		resp, err := postData(node.DataLocation, chunk.RP, chunk.DB, chunk.Buf)
		if err != nil {
			return fmt.Errorf("Failed to recover data for node %s. Got error: %s", node.Name, err.Error())
		}
		if resp.StatusCode > 204 {
			return fmt.Errorf("Failed to recover data for node %s. Received response code: %d", node.Name, resp.StatusCode)
		}
 	}
	return nil
}

func isAlive(location string, client *http.Client) bool {
	values, _ := url.ParseQuery("q=SHOW DATABASES")
	resp, err := client.Get("http://" + location + "/query?q=" + values.Encode())
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