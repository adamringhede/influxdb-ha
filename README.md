# InfluxDB Cluster (unofficial)
A distributed clustering system for production deployments of InfluxDB with high availability and horizontal scalability via partitioning.

*Partitioning is not different from what is often called "sharding" in NoSQL database systems. However, InfluxDB already uses the term "sharding" to refer to
splitting individual series into chunks, called chards, based on time. In order to scale writes, it would not work to shard data using only time chunks as most writes would be made
to the same chunk on the same node. Instead, in this implementation, entire series are distributed to different nodes which we call "partitioning".* 

## Etcd
The cluster is dependent on etcd for storing data used for clustering mechanisms. It is preferable 
to use a cluster of 3 etcd instances for high availability but it is not required as the cluster can function with degraded functionality while Etcd is unavailable.

## Extend existing deployment

### High availability via replication
By adding one ore more nodes, data will automatically be replicated

#### 1. Start a cluster agent 
Start a cluster agent process referencing the old - still running - single node on the same 
machine using the `-data` option. The ip should be reachable from all other nodes in the cluster 
as well for replication and forwarding of queries. 

```
influxc -data 10.2.3.4:8086 -cluster-id 1 -etcd "10.3.4.5:2379,10.4.5.6:2379,10.5.6.7:2379"
```

In order to migrate data in InfluxDB, the cluster agent needs to have admin access

#### 2. Add more nodes
On another machine, start InfluxDB and the clustering agent like above but with a new 
influxd process. The cluster-id has to be the same and it should also specity the etcd nodes.
Data will then automatically start being imported to the new node. 

##### Recovery of passwords
With an existing single node deployment, all meta data needs to be replicated which includes users and passwords. If the deployment is set up in a way that the cluster agent can’t access meta data files where hashed passwords are stored, then it will not be able to recover passwords, and all users' passwords will be reset and need to be set again manually. 

##### Greater number of nodes than replication factor
If you add more nodes than the configured replication factor, the additional nodes can be used as well if you have more than one measurement. If you only have a few very large measurements and some other much smaller, some nodes will not be used to its fullest capacity and it could be more beneficial in terms of hardware costs to host the smaller measurements seperately in more appropriate instances. 

### Scale via partitioning
Partitioning is possible to do on a running InfluxDB deployment without downtime. Note that after it has moved some of the data to the new nodes, it will delete the data from the original node permanently, so it is recommended to make a backup before transitioning to a partitioned deployment. 


#### 1. Start a cluster agent 
Start a cluster agent process referencing the old single node on the same machine.

```
influxc -data 10.2.3.4:8086 -cluster-id 1 -etcd "10.3.4.5:2379,10.4.5.6:2379,10.5.6.7:2379"
```

#### 2. Create a partition key
Create a partition key on the database or database and measurement combination. Read **Selecting partition key tags** before doing this.

```sql
CREATE PARTITION KEY meter_id,region ON mydb.mymeasurement
```

#### 3. (optional) Change replication factor
By default, data will be replicated to 2 nodes. So if the deployment consists of 2 nodes, then partitioning does not have any effect until another node is added. The replication factor can be changed later, however it is preferable to set it to the preferred number now rather than after the data is partitioned to not spend time on moving around data.

The replication factor can either be defined as a default, on a single database or just on a specific measurement by specifying an `ON` clause

Changing the default to replicate to 3 nodes
```sql
SET REPLICATION FACTOR 3
```

Setting the replication factor to 1 to avoid replication in order to preserve storage space on a single measurement. 
```sql
SET REPLICATION FACTOR 1 ON mydb.mymeasurement
```

For a measurement that receives a lot of read from different systems or users at the same time, you may want to increase the replication factor. 
Note that, if you have less nodes in the cluster than the set replicatin factor, then it will not replicate more times than there are nodes. 
As more nodes are added, data will automatically be replicated until the number of replicas requested by the replication factor is met. 
```sql
SET REPLICATION FACTOR 10 ON mydb.mymeasurement
```

#### 4. (optional) Add more nodes
On another machine, start InfluxDB and the clustering agent like above but with the new influxdb process. The cluster-id has to be the same and it should also specity the etcd nodes.

About 50% of the data will then automatically start being imported to the new node. After the data is imported, it will be deleted from from the original node unless that node should still have some of the data according to the replication factor.

Repeat the last step until you have as many nodes as you want.

*A complete repartitioning is not required when adding nodes as this implementation is using what's called "consistent hashing" which makes adding another node require has a constant duration, rather than a linear increase. This makes adding and removing nodes efficient.*

## Selecting partition key tags
A partition key requires one or more tags to partition data. To be able to query partitioned data efficiently without having to broadcast the query the entire cluster the tags need to be in the `WHERE` clause of the query in `=` conditions. That means having fewer tags can give more freedom when making queries. However, if the tag has low cardinality or very disproportionate, then it may not be possible to partition the data evenly across the nodes in the cluster. This can then be resolved by adding another tag to the partition key. 

Changing partition key later is possible – even without downtime – but requires creating a new measurement and copying all data to that measurement where it will be distributed differently. 

## Managing nodes
The following commands can be used in the normal influx client by connecting to any of the nodes. 

### Show nodes
Get a list of all nodes currently in the cluster.

```sql
SHOW NODES
```

### Removing a node
Removing a node will cause other nodes to import data as needed from it. If the data is not replicated (that is the replication factor is set to 1), then it is important that the underlying influxd process is up so that the other nodes can import data from it. 

```sql
REMOVE NODE nodename
```

## Hardware sizing guidelines
The guidelines for single nodes given by the official InfluxDB docuentations can be extrapolated here. The cluster agents do not add much overhead, but need additional storage to save data temporarily that could not be written when the target node is unavailable.

### Upgrading storage
There are two main ways of upgrading the storage capacity which should work regardless of your infrastructure. 

The first, which is commonly used for database deployments, is to stop the node, attach a new larger volume, copy the data to it from the old one and then start it.
Data that was to be written to it while it was down will be recovered after starting the node.
Note that this implementation distributes data evenly; so to increase storage capacity, all nodes need to be upgraded  

The second, which does not require stopping any node and may be more appropriate if I/O rate is a bottleneck  is
to add one or more nodes. 
If you have more nodes than the configured replication factor, data will be distributed evenly as another node is added.

### Recovery data storage 
The cluster agent process need access to additional persistent storage for recovery data. The amount required depends on the volumes of points written when a node is unavailable and how fast it can be recovered which depends on disk io.    

## Distributed queries

### Query to multiple partitions without aggregations
The query is distributed to nodes and results are then merged and sorted to give the impression that the request was made to a single node.  

### Query to multiple partitions with aggregations
Aggregations make it a lot more complicated but is possible and will be completely automatic. The query is first decompiled and an abstract syntax tree (AST) is created that is then
used to perform the aggregation from results from the nodes. 

## Limitations

### Sub queries
Currently does not work at all

Future:
* May work on partitioned data if the entire query can be contained to a single node or set of nodes

### Multiple FROM clauses
Not supported when querying partitioned data but works for other nodes

Future:
* Can work by combining the results locally.

