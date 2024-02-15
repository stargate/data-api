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
- `connections` - number of HTTP2 connections to be shared between the threads (default: `20`) 


