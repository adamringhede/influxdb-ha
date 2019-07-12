# Kubernetes setup 

InfluxDB Cluster is designed to be easy to run using container orchestrators such as Kubernetes.

Kubernetes configurations to run it has been provided, which can be used in production.

As Etcd is required to run the cluster, an etcd configuration has is included as well. 
If you already have an Etcd deployment, you can use that one. Just be sure to update the etcd configuration
so that it refers to the correct etcd hosts. 

## Run the example

```bash
kubectl create -f etcd.yaml
kubectl create -f influxc.yaml
```
This will create cluster of a stateful set with 3 nodes. Scaling up with more nodes
will automatically trigger the redistribution of data. Scaling down, at the moment, does not trigger
redistribution and the nodes need to be removed individually by an admin. If replication is 
configured to be greater than or equal to 2 replicas, then it is safe to remove nodes. If not, 
manually remove the node, wait for all imports in the cluster to complete, then remove the node.
If you don't do this, then the data will be deleted and difficult to recover. 
```sql
REMOVE NODE <hostname>
```


## Debug deployment


#### Connect to cluster via influx
```
influx -host $(minikube service influxc --format "{{.IP}}") -port $(minikube service influxc --format "{{.Port}}")  
```

When the cluster is first started, you need to create an admin user. Then authenticate by typing `auth` and enter the credentials 'admin' and 'password' 
```sql
> CREATE USER admin WITH PASSWORD 'password' WITH ALL PRIVILEGES
> auth
username: admin
password: 

```

Show what nodes are currently in the cluster. 
```sql
> SHOW NODES
name: nodes
name             data location
----             -------------
influx-cluster-0 influx-cluster-0.influxc:8086
influx-cluster-1 influx-cluster-1.influxc:8086
influx-cluster-2 influx-cluster-2.influxc:8086

```


#### Dump meta data
```
ETCDCTL_API=3 etcdctl --endpoints "$(minikube service etcd --url)"  get "" --prefix
```