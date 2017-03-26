### Commands
```
curl -i -XPOST http://$DOCKER_IP:8086/query --data-urlencode "q=CREATE DATABASE mydb"

curl -i -XPOST 'http://192.168.99.100:8086/write?db=mydb' --data-binary 'cpu_load_short,host=server01,region=us-west value=0.64 1434055562000000000'

curl -i -XPOST 'http://192.168.99.100:9096/write?db=mydb' --data-binary 'cpu_load_short,host=server01,region=us-west value=0.11 1455055562000000000'

curl -G 'http://192.168.99.100:8086/query?pretty=true' --data-urlencode "db=mydb" --data-urlencode "q=SELECT \"value\" FROM \"cpu_load_short\" WHERE \"region\"='us-west'"




Chunked test

curl -Gi 'http://192.168.99.100:8086/query?pretty=false' --data-urlencode "db=r" --data-urlencode "q=SELECT \"value\" FROM \"cpu_load_short\" GROUP BY *" --data-urlencode "chunked=true"



Router


curl -i -XPOST 'http://localhost:5096/write?db=mydb' --data-binary 'cpu_load_short,host=server01,region=us-west value=0.64 1434055562000000000'
curl -G 'http://localhost:5096/query?pretty=true' --data-urlencode "db=mydb" --data-urlencode "q=SELECT \"value\" FROM \"cpu_load_short\" WHERE \"region\"='us-west'"



```

The database has to be created first on each instance. 

### API

#### Create a new cluster

POST /clusters

This creates a new service with the specified number of replicas and relays. Changing these values in the future with reconfigure the cluster. 

After a replica is launched, a worker running the reconfiguration job makes a request to the server to create the database. If the worker fails, it will be restarted. The job can be picked up by any worker.
Alternatively, after a create job is launched, a token is returned to the client that can be used to authenticate with the cluster, then the cluster connects to the cluster and performs the jobs.

The client polls the API for changes to the state. Alternatively, a websocket can be used.

### Resource and log monitoring

Each instance is writing its resource usage to a special database as well as events that occurs. 

### Router

A router takes care of splitting traffic between the relays and replicas. 
It will eventually be used to support sharding between clusters. 
There can be one or more routers. They know where to route traffic by looking up config servers, which could be something like redis.
As soon as a configuration is updates, this will be broadcasted via rabbitmq, which the routers need to be connected to.

### Workers

Workers take care of performing jobs for maintaining the clusters. 
They subscribe to task messages from rabbit mq so that only one worker can work on something at a time.

Example tasks:
* Create databases on all nodes after a new cluster is up.
* Perform rolling updates of database version and hardware
* Re-balance shards between replica sets.
* Perform backup
* Recover data after downtime. (write data into shards)
* Recover new nodes when increasing the number of replicas
* Import historical data from new continuous queries if requested.


## AWS Setup

Developer selects how many machines and storage to provision by deciding on a setup.
Each machine will run CoreOS

## Kubernetes Setup

* Statefulset for data nodes. 
* Stateless service for relays. Or relays can be run on data nodes. 
* Stateless service for routers. It should discover nodes through cluster hostnames with a kubernetes data node resolver,
  which otherwise can just be a config file.

A router service sends requests to one of the routers. The router needs to be aware of all data nodes.
A router routinely checks for new data nodes in a kubernetes setup. Otherwise it will need a config file
and be restarted, or it can also watch that configfile for changes during runtime.

The router can be configured to distribute the reads to a single data node service. 
For writes it needs to know about all of the relays, so it may be simpler to just keep the relay on the data nodes.
For database management, the router need to know about all the datanodes so that it can perform the same requests on all of them.
Like creating a new database, managing users, etc. A client can make these queries against any router.
The router checks the content of the queries to determine if they are normal read queries or administrative queries that 
should be made to all the datanodes.


### Upgrading storage
To upgrade the storage space without downtime can be accomplished by creating a new
replica, directing writes to it through relays but not reads, meanwhile recover data
to it from the backup of an old replica.
Maybe it is possible to create a new statefulset with other larger pvc and then gracefully
transfer the traffick to that one.

#### Only way I see to upgrade storage
is to perform a forced replace:
kubectl replace --force -f upgrade.yaml

The upgrade configuration includes another volumeMount for a pvc with the requested amount of storage.
During the upgrade, relays should also write to the