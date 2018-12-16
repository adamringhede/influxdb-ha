package syncing

import (
	"fmt"
	"github.com/adamringhede/influxdb-ha/cluster"
	"strings"
)

// need to get series based on one partition key and location

// need to test if these matches one or more tokens

type Series struct {
	Measurement string
	Tags        map[string][]string
}

func NewSeriesFromKey(key string) (series Series) {
	var parts = strings.Split(key, ",")
	series.Measurement = parts[0]
	var tags = parts[1:]
	series.Tags = make(map[string][]string, len(tags))
	for _, tag := range tags {
		var tagParts = strings.Split(tag, "=")
		series.Tags[tagParts[0]] = []string{tagParts[1]}
	}
	return series
}

func (s Series) Where() string {
	var result []string
	for key, value := range s.Tags { // TODO create a version of this that only includes the tags in a partition key.
		result = append(result, fmt.Sprintf("%s='%s'", key, value[0]))
	}
	return strings.Join(result, " AND ")
}

func (s Series) Matches(token int, pk cluster.PartitionKey, resolver *cluster.Resolver) bool {
	if pk.Tags == nil {
		return false
	}
	hash, err := cluster.GetHash(pk, s.filterTagsByPartitionKey(pk))
	if err != nil{
		return false
	}
	resolvedToken, ok := resolver.FindTokenByKey(hash)
	if !ok {
		return false
	}
	return resolvedToken == token
}

func (s Series) filterTagsByPartitionKey(pk cluster.PartitionKey) (map[string][]string) {
	result := map[string][]string{}
	for _, tag := range pk.Tags {
		if tagValue, hasTag := s.Tags[tag]; hasTag {
			result[tag] = tagValue
		}
	}
	if len(result) == 0 {
		panic("empty filtered tags")
	}
	return result
}

func FetchSeries(location string, db string) (series []Series, err error) {
	offset := 0
	limit := 1000
	for {
		results, fetchErr := fetchSimple(fmt.Sprintf(`SHOW SERIES LIMIT %d OFFSET %d`, limit, offset), location, db)
		if fetchErr != nil {
			return series, fetchErr
		}
		for _, result := range results {
			for _, resultSeries := range result.Series {
				for _, key := range resultSeries.Values {
					series = append(series, NewSeriesFromKey(key[0].(string)))
				}
			}
		}
		offset += limit
		if len(results) == 0 || len(results[0].Series) == 0 {
			break
		}
	}
	return series, nil
}
