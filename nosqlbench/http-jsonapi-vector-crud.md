# JSON API Vector CRUD

## Description

The JSON API CRUD Dataset workflow targets Stargate's JSON API using JSON documents from an external dataset.
The [dataset](#dataset) is mandatory and should contain a vector per row that should be used as the input for write, read and update operations.
This workflow is perfect for testing Stargate performance using your own JSON dataset or any other realistic dataset.

In contrast to other workflows, this one is not split into ramp-up and main phases.
Instead, there is only the main phase with 4 different load types (write, read, update and delete).

## Named Scenarios

### default

The default scenario for http-jsonapi-vector-crud.yaml runs each type of the main phase sequentially: write, read, update and delete. This means that setting cycles for each of the phases should be done using the: `write-cycles`, `read-cycles`, `update-cycles` and `delete-cycles`. The default value for all 4 cycles variables is the amount of documents to process (see [Workload Parameters](http://localhost:63342/markdownPreview/147307353/markdown-preview-index-1841516304.html?_ijt=avuea5chkg34krn8blmr2k7431#workload-parameters)).

Note that error handling is set to `errors=timer,warn`, which means that in case of HTTP errors the scenario is not stopped.

## Dataset

### Vector Sample

Vector size is 1536 in the bench file. (openAI embedding vector standard size is 1536)
Sample dataset is in [vector dataset](vector-dataset.txt)

> If you want to test different vector-size, please change [http-jsonapi-vector-crud](http-jsonapi-vector-crud.yaml) and [vector dataset](vector-dataset.txt)

## Workload Parameters

- `docscount` - the number of documents to process in each step of a scenario (default: `500`)
- `dataset_file` - the file to read the JSON documents from (note that if number of documents in a file is smaller than the `docscount` parameter, the documents will be reused)
- `connections` - number of HTTP2 connections to be shared between the threads (default: `20`) 

note that too many docscount requires larger storage for cassandra backend 
