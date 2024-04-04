# JSON API Vectorize

## Description
The NosqlBench workload to test vectorize feature

## Named Scenarios

### default

The default scenario for http-jsonapi-vectorize-test.yaml runs each type of the main phase sequentially: write, read, update and delete. This means that setting cycles for each of the phases should be done using the: `write-cycles`, `read-cycles`, `update-cycles` and `delete-cycles`.
Note that error handling is set to `errors=timer,warn`, which means that in case of HTTP errors the scenario is not stopped.

## Dataset

### Movie Description Sample

Sample dataset is in [vectorize dataset](vectorize-dataset.txt), which contains descriptions for 100 famous movie 

> For testing different embedding service, please change schema block with model and dimension

## Workload Parameters

- `connections` - number of HTTP2 connections to be shared between the threads (default: `20`)
- `write-cycles`, `read-cycles`, `update-cycles`,`delete-cycles` - running cycles for each phases (default: `10000`)

## Sample Command

### Against AstraDB

```
nb5 -v http-jsonapi-vectorize-crud docscount=1000 threads=20 jsonapi_host=Your-AstraDB-Host auth_token=Your-AstraDB-Token jsonapi_port=443 protocol=https path_prefix=/api/json namespace=Your-Keyspace
```

### Against Local JSON API

```
nb5 -v http-jsonapi-vectorize-crud jsonapi_host=localhost docscount=1000 threads=20
```

