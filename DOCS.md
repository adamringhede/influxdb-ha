# InfluxDB cluster (unofficial)
A distributed clustering system for production deployments of InfluxDB with high availability and linear scalability via partitioning.

## Etcd
The cluster is dependent on etcd for storing data critical to the cluster. It is preferable to use a cluster of 3 etcd instances for high availability but it is not required as it can function with degraded functionality while Etcd is unavailable.

## Extend existing deployment

### High availabity via replication
By adding one ore more nodes, data will automatically be replicated

1. Start a cluster agent process referencing the old single node on the same machine using the `-data` option. The ip should be reachable from all other nodes in the cluster.

```
influxc -data 10.2.3.4:8086 -cluster-id 1 -etcd "10.3.4.5:2379,10.4.5.6:2379,10.5.6.7:2379"
```

2. On another machine, start InfluxDB and the clustering agent like above but with the new influxd process. The cluster-id has to be the same and it should also specity the etcd nodes.

Data will then automatically start being imported to the new node. 

#### Greater number of nodes than replication factor
If you add more nodes than the configured replication factor, the additional nodes can be used as well if you have more than one measurement. If you only have a few very large measurements and some other much smaller, some nodes will not be used to its fullest capacity and it could be more beneficial in terms of hardware costs to host the smaller measurements seperately in more appropriate instances. 

### Scale via partitioning
Partitioning is possible to do on a running InfluxDB deployment and also does not require any downtime. Note that after it has moved some of the data to the new nodes, it will delete the data from the original node permanently, so it is recommended to make a backup before transitioning to a partitioned deployment. 


#### 1. Start a cluster agent 
STart a cluster agent process referencing the old single node on the same machine

```
influxc -data 10.2.3.4:8086 -cluster-id 1 -etcd "10.3.4.5:2379,10.4.5.6:2379,10.5.6.7:2379"
```

#### 2. Create a partition key
Create a partition key on the database or database and measurement combination. Read **Selecting partition key tags** before doing this.

```sql
CREATE PARTITION KEY meter_id,region ON mydb.mymeasurement
```

#### 3. (optional) Change replication factor
By default, data will be replicated to 2 nodes. So if the deployment consists of 2 nodes, then partitioning does not have any effect until another node is added. The replication factor can be changed later, however it is preferrable set it to the preferred number now rather than after the data is partitioned. 

The replication factor can either be defined as a default or on a single database or just on a specific measurement by specifying an `ON` clause

Changing the default to replicate to 3 nodes
```sql
SET REPLICATION FACTOR 3
```

Setting the replication factor to 1 to avoid replication in order to preserve storage on a single measurement. 
```sql
SET REPLICATION FACTOR 1 ON mydb.mymeasurement
```

Setting the replication factor to a high number for many reads. If the same data is needed by many users, it can be preferred to replicate it to many nodes. 
```sql
SET REPLICATION FACTOR 10 ON mydb.mymeasurement
```

#### 4. Add more nodes
On another machine, start InfluxDB and the clustering agent like above but with the new influxdb process. The cluster-id has to be the same and it should also specity the etcd nodes.

About 50% of the data will then automatically start being imported to the new node. After the data is imported, it will be deleted from from the original node unless that node should still have some of the data according to the replication factor.

Repeat the last step until you have as many nodes as you want.

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

### Recovery data storage 
The cluster agent process need access to persistent storage. The amount required depends on the volumes of points written when a node is unavailable and how fast it can be recovered which depends on disk io.    

## Distributed queries

### Query to multiple partitions without aggregations
The query is distributed to nodes and results are then merged and sorted to give the impression that the request was made to a single node.  

### Query to multiple partitions with aggregations
Aggregations make it a lot more complicated. The query is first decompiled and a custom AST (abstract syntax tree) is created that is then
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

