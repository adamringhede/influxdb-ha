package cluster

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
)

type RecoveryStorage interface {
	// Put should save data so that it later can be sent to the data node when it recovers
	Put(nodeName string, buf []byte) error
	// Get will return a channel of for streaming data for a certain node
	Get(nodeName string) chan []byte
}

type LocalRecoveryStorage struct {
	Path  string
	hints HintStorage
	// files contain file descriptors for writing data used for node recovery
	files map[string]*os.File
}

// NewLocalStorage create a LocalRecoveryStorage for saving data at the specified path in different files.
// A hint storage can be provided in order to save where data is placed for recovery.
func NewLocalStorage(path string, hs HintStorage) *LocalRecoveryStorage {
	return &LocalRecoveryStorage{path, hs, map[string]*os.File{}}
}

func (s *LocalRecoveryStorage) getFilePath(nodeName string) string {
	return filepath.Join(s.Path, createFilename(nodeName))
}

func (s *LocalRecoveryStorage) getOrCreateFile(nodeName string) (*os.File, error) {
	if f, ok := s.files[nodeName]; ok {
		return f, nil
	}
	filePath := s.getFilePath(nodeName)
	f, err := os.OpenFile(filePath, os.O_WRONLY, os.ModeAppend)
	if _, ok := err.(*os.PathError); ok {
		f, err = os.Create(filePath)
	}
	if err != nil {
		return f, err
	}
	s.files[nodeName] = f
	return f, err
}

func (s *LocalRecoveryStorage) Put(nodeName string, buf []byte) error {
	f, err := s.getOrCreateFile(nodeName)
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

func (s *LocalRecoveryStorage) Drop(nodeName string) error {
	err := os.Remove(s.getFilePath(nodeName))
	return err
}

func (s *LocalRecoveryStorage) Get(nodeName string) (<-chan []byte, error) {
	f, err := os.Open(s.getFilePath(nodeName))
	if err != nil {
		return nil, err
	}
	ch := make(chan []byte)
	go func() {
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			buf := scanner.Bytes()
			if len(string(buf)) != 0 {
				ch <- buf
			}
		}
		close(ch)
		f.Close()
	}()
	return ch, nil
}

func (s *LocalRecoveryStorage) Close() {
	for _, f := range s.files {
		f.Close()
	}
}

func createFilename(nodeName string) string {
	return fmt.Sprintf("recovery_data_%s", nodeName)
}
