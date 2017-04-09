package sharding

type Chunk struct {
	Measurement string
	Nodes       []string
}

type KeyPart struct {
	Tag  string
	Type string
}

type Measurement struct {
	ShardKey          []KeyPart
	ReplicationFactor int
}

// todo: struct with methods for interacting with etcd. a sharding service.
// todo: given a set of tags, it should be able to resolve a list of hosts to send the query to.

/* to find where the data is:
a query comes in.
it has a condition like foo = 55 on measurement M
the tag foo matches the shard key for M
A hash is calculated giving value 123857103.
This value is used to find the chunk with % (modulo)
This is possible if each chunk has an id ranging from 0 to n.
That chunk, which references one or more nodes, is retrieved from etcd and the query is routed to one of the hosts.
In this case a chunk can grow to any size, and it is not possible to split it as it is not based on a range.
The only way to split it is to increase "n". The question is, how can n be changed during runtime?
If we increase the number of chunks.
The reason to increase n is to be able to spread chunks across many more servers. This is equivalent to
primary shards in ElasticSearch.

Larger amounts of shards

The issue with this model is that if some series is a lot bigger than others, chunks could be of very
different sizes as it is not possible to split them. For normal usage this is most likely not a problem,
and if it is, the user can maybe split it into multiple series or store data for it separately.

314234 % n = 5
Get all series, determine what chunks they should be placed on. each chunk

Let's say there are 1 000 000 possible chunks, each storing up to 128 MB of data, then that should handle 128 TB of data
Not all chunks need to be created and allocated servers until data is actually stored on them.

With many chunks however, it is possible that every series requested will require a separate query for where to get the data.
It rebalancing needed?

The other method is to represent all chunks as a range. The benefit is that with a lot more series and we need
less chunks. For this to work though, all routers would need to keep a copy of all chunks intervals to be able
to select the correct chunk on queries. This may not be a problem. How much memory does it really take to store a million chunks? If each chunk is 64 MB,
then that is 64 TB of data.



there is an argument for having chunks include data for multiple measurements.
if a query is on multiple measurements, we don't want to contact more than necessary servers.


from 8 bytes, to 8 bytes, 24 bytes = 40 bytes.


40 bytes * 1 million = 40 MB can handle 64 TB of data

1 peta byte of data would need 1.6 GB of meta data storage.


*/
