# JSON API Soak Test

## Description

This soak test workload for jsonapi.

The example JSON looks like:

```json
{
  "_id" : "xxx",
  "user_id":"56fd76f6-081d-401a-85eb-b1d9e5bba058",
  "created_on":1476743286,
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
  "debt":null,
  "match1": 0, // or 1000
  "match2": "true", // or "false"
  "match3": true // or false,
  "$vector": [...] // vector array with specified dimension
}
```



## Workload Parameters

- `docscount` - the number of documents to write during rampup (default: `10_000_000`)
- `docpadding` - the number of fields to add to each document; useful for writing larger documents. A value of e.g. `5` would make each document have 20 leaf values, instead of 15. (default: `0`)
- `match-ratio` - a value between 0 and 1 detailing what ratio of the documents written should match the search parameters. If match-ratio is e.g. `0.1` then approximately one-tenth of the documents will have `match1`, `match2`, and `match3` values that are `0`, `"true"`, and `true`, respectively. (default: `0.01`)
- `fields` - the URL-encoded value for `fields` that you would send to the Docs API. This restricts the fields returned during benchmarking.
- `connections` - number of HTTP2 connections to be shared between the threads (default: `20`) 


