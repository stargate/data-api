# JSON API Vector InsertMany

## Description

The JSON API insertMany(ordered false) performance test workload

In contrast to other workflows, this one is not split into ramp-up and main phases. Instead, there is only the write phase.

## Named Scenarios

### default

The default scenario for http-jsonapi-vector-insertmany.yaml only has one operation - insert 20 records to the database
a time.

## Dataset

### Vector Sample

Vector size is 1536 in the nosqlbench file. (openAI embedding vector standard size is 1536)


## Sample Command

### Against AstraDB

```
nb5 -v http-jsonapi-vector-crud docscount=1000 threads=20 jsonapi_host=Your-AstraDB-Host auth_token=Your-AstraDB-Token jsonapi_port=443 protocol=https path_prefix=/api/json keyspace=Your-Keyspace
```

### Against Local JSON API

```
nb5 -v http-jsonapi-vector-crud jsonapi_host=localhost docscount=1000 threads=20
```

