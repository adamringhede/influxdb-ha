## Shard key

The shard keys contains one or more tags. The sharding strategy can be "range" or "hashed".
Range keys only work for numerical values.
We could potentially maybe support it for strings as well, but its application is hard.

### Hashed keys (this does not work)
To resolve the shard using a hashed shard key, a numerical value ranging from 0 to _n_ - 1, where _n_
is the number of chunks, is calculated by getting the remainder from toInt(hash) % n. Each chunk
is placed on some shard in the cluster. What chunks belong to each shard is stored in a replicated
config server with consistency guarantees.


The hashed value is added as a tag to each point so that it is possible to find those points in the database
for migrations. For chunk splitting we need to know how many values there are in each chunk.

#### Hashed to numerical for ranged
The tag value is hashed to a numerical value. This value then used to determine what chunk to write
the value to. In this way, the same logic for storing ranged keys can be used instead of using modulo. I
am not sure if that is harder or just as easy.

### Ranged keys
To avoid broadcast queries with range requests on numerical tags, ranged shard keys can limit the number of shards
that will be queried. Each chunk is allocated a certain range. The router needs to keep track of the min and max
values for that tag, so that it can figure out what ranges to use.

The ranges of the ranged component in a shard key is determined based on the number of elements in each shard.
If the number of elements in a chunk reaches a certain point, the ranges are reevaluated.
The count of values in each chunk must therefore be stored.

A leader periodically polls the sizes of chunks to determine if it should rebalance chunks
across shards or reevaluate the chunks.

## Selecting shard/partition based on Option



### Shard key of type String
When the shard key is of type String,



### Config Storage
Configurations of chunks is stored in etcd.

They are stored like: chunks/<db>_<measurement>_<shard-key>_<chunk-id>

## Sharding continuous queries
If a CQ is created for a measurement that already is sharded, then the resultant measurement from
the continuous query will naturally be partitioned as well. However, it is important then that
chunk migrations also migrate the data of the continuous query measurement for the same tags
used in the original sharding. Inserting and dropping series on a CQ's measurement works just
like normal measurements.


## Merge behaviour
Results from multiple shards can be merged,




## How to shard


### Shard a measurement
Use an HTTP request on the router to shard a measurement. The request includes the database and
measurement to shard on, as well as the sharding key used to partition the data across chunks.

```
POST /cluster/measurements/shard
{
    "db": "pirate",
    "measurement": "treasures",
    "key": [{tag: "captain", type: "hash"}]
}
or
{
    "db": "pirate",
    "measurement": "treasures",
    "key": [{tag: "price", type: "range"}]
}
```

A record will be stored in the config database about the sharded measurement.

If the measurement is not empty, it will all be assigned to one chunk, which is then partitioned.
Chunks are stored in the config database and references a shard. The shard in turn is aware of
all nodes in the replicaset assigned to it.

### Create a shard
To distribute the data, shards need to be added to the cluster.
The shards needs to know the addresses of both databases and relays.
The shard can later be updated to include more or less nodes.

```
POST /cluster/shards
{
    "hosts": ["db-a1:8086", "db-a2:8086"]
}
```

As data is written through the router, chunk look-ups are made to the config database.
The best architecture consists of 2 or more routers, which etcd instances on each.
For each replica set / shard, two relays and two data nodes.
