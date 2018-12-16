package service

import (
	"fmt"
	"github.com/adamringhede/influxdb-ha/cluster"
	"github.com/adamringhede/influxdb-ha/hash"
	"github.com/influxdata/influxdb/models"
	"strings"
)

type PointsWriter interface {
	WritePoints(points []models.Point, locations []*cluster.Node, writeContext WriteContext) error
}

type WriteContext struct {
	precision string
	db string
	rp string
}

type partitionValidationError error

func partitionPoints(points []models.Point, partitioner cluster.Partitioner, db string) (map[int][]models.Point, error) {
	pointGroups := make(map[int][]models.Point)
	for _, point := range points {
		key, ok := partitioner.GetKeyByMeasurement(db, string(point.Name()))
		var numericHash int
		if ok {
			values := make(map[string][]string)
			for _, tag := range point.Tags() {
				values[string(tag.Key)] = []string{string(tag.Value)}
			}
			if !partitioner.FulfillsKey(key, values) {
				err := partitionValidationError(fmt.Errorf("the partition key for measurement %s requires the tags [%s]",
						key.Measurement, strings.Join(key.Tags, ", ")))
				return pointGroups, err
			}

			numericHash, _ = cluster.GetHash(key, values)
		} else {
			numericHash = int(hash.String(cluster.CreatePartitionKeyIdentifier(db, "")))
		}
		if _, ok := pointGroups[numericHash]; !ok {
			pointGroups[numericHash] = []models.Point{}
		}
		pointGroups[numericHash] = append(pointGroups[numericHash], point)
	}
	return pointGroups, nil
}
