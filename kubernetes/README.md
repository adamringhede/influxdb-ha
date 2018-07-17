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