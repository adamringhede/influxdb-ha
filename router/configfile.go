package router

import (
	"errors"
	"github.com/BurntSushi/toml"
	"io/ioutil"
)

type node struct {
	Name     string `toml:"name"`
	Location string `toml:"location"`
}

type RouterConfig struct {
	Name     string
	BindAddr string `toml:"bind-addr"`
	Relays   []node `toml:"relays"`
	Data     []node `toml:"dbs"`
}

func (r *RouterConfig) Validate() error {
	if len(r.Relays) == 0 {
		return errors.New("Must have at least one relay")
	}
	if len(r.Data) == 0 {
		return errors.New("Must have at least one data node")
	}
	return nil
}

type ReplicaSet struct {
	Name     string `toml:"name"`
	Replicas []node `toml:"replica"`
}

type FileConfig struct {
	Routers     []RouterConfig `toml:"router"`
	ReplicaSets []ReplicaSet   `toml:"replicaSet"`
}

func ParseConfigFile(filename string) (FileConfig, error) {
	file, err := ioutil.ReadFile(filename)
	var config FileConfig
	if err != nil {
		return config, err
	}
	_, decodeErr := toml.Decode(string(file), &config)
	if decodeErr != nil {
		return config, decodeErr
	}
	/*for _, r := range config.Routers {
		validationErr := r.Validate()
		if validationErr != nil {
			return config, validationErr
		}
	}*/
	return config, nil
}
