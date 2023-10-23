# JSON API Vector InsertMany

## Description

The JSON API insertmany Dataset workflow targets Stargate's JSON API using JSON documents from an external dataset.
The [dataset](#dataset) is mandatory and should contain a vector per row that should be used as the input for write, read and update operations.
This workflow is perfect for testing Stargate performance using your own JSON dataset or any other realistic dataset.

In contrast to other workflows, this one is not split into ramp-up and main phases. Instead, there is only the write phase.

## Named Scenarios

### default

The default scenario for http-jsonapi-vector-insertmany.yaml only has one operation - insert 20 records to the database
a time.

Note that error handling is set to `errors=timer,warn`, which means that in case of HTTP errors the scenario is not stopped.

## Dataset

### Vector Sample

Vector size is 1536 in the nosqlbench file. (openAI embedding vector standard size is 1536)
Sample dataset is in [vector dataset](vector-dataset.txt)

> If you want to test different vector-size, please change [http-jsonapi-vector-crud create-collection op](http-jsonapi-vector-crud.yaml) and [vector dataset](vector-dataset.txt)


## Sample Command

### Against AstraDB

> comment out `create-namespace` op in the [nosqlbench yaml file](http-jsonapi-vector-crud.yaml)

```
nb5 -v http-jsonapi-vector-crud docscount=1000 threads=20 jsonapi_host=Your-AstraDB-Host auth_token=Your-AstraDB-Token jsonapi_port=443 protocol=https path_prefix=/api/json namespace=Your-Keyspace
```

### Against Local JSON API

```
nb5 -v http-jsonapi-vector-crud jsonapi_host=localhost docscount=1000 threads=20
```

