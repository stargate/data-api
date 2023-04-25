# JSON API search, filter and sort

## Description

The JSON API search, filter and sort workflow targets Stargate's JSON API using generated JSON documents.
The operations in this workflow uses find commands with different filter clause, sort clause and projection clause.
The documents used are sharing the same structure and are approximately half a kilobyte in size each:

* each document has 13 leaf values, with a maximum depth of 3
* there is at least one `string`, `boolean`, `number` and `null` leaf
* there is one array with `double` values and one with `string` values
* there is one empty array and one empty map

The example JSON looks like:

```json
{
  "user_id":"56fd76f6-081d-401a-85eb-b1d9e5bba058",
  "created_on":1476743286,
  "gender":"F",
  "full_name":"Andrew Daniels",
  "married":true,
  "address":{
    "primary":{
      "cc":"IO",
      "city":"Okmulgee"
    },
    "secondary":{

    }
  },
  "coordinates":[
    64.65964627052323,
    -122.35334535072856
  ],
  "children":[

  ],
  "friends":[
    "3df498b1-9568-4584-96fd-76f6081da01a"
  ],
  "debt":null
}
```

## Named Scenarios

### schema

Creates namespace and collections for the JSON API testing

### rampup

Writes documents to the database and runs read test using id.

### main

Runs find command that can return multiple documents. Find command is run with different options
* eq conditions that use secondary index
* eq conditions on multiple fields that use secondary index
* use $exists operators that check if field exists
* use $size operator that check if array has specific size
* use sort, performs in memory sort based on the field in the request
* use sort with limit and skip options with sort
* use projection clause to return only specific fields


Note that error handling is set to `errors=timer,warn`, which means that in case of HTTP errors the scenario is not stopped.

## Workload Parameters

- `docscount` - the number of documents to process in each step of a scenario (default: `10_000_000`)
- `connections` - number of HTTP2 connections to be shared between the threads (default: `20`)

Note that if number of documents is higher than `read-cycles` you would experience misses, which will result in `HTTP 404` and smaller latencies.


