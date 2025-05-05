package io.stargate.sgv2.jsonapi;

import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.config.constants.OpenApiConstants;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Application;
import org.eclipse.microprofile.openapi.annotations.Components;
import org.eclipse.microprofile.openapi.annotations.OpenAPIDefinition;
import org.eclipse.microprofile.openapi.annotations.enums.ParameterIn;
import org.eclipse.microprofile.openapi.annotations.enums.SecuritySchemeIn;
import org.eclipse.microprofile.openapi.annotations.enums.SecuritySchemeType;
import org.eclipse.microprofile.openapi.annotations.info.Info;
import org.eclipse.microprofile.openapi.annotations.media.ExampleObject;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.security.SecurityScheme;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

@OpenAPIDefinition(
    // note that info is defined via the properties
    info = @Info(title = "", version = ""),
    tags = {
      @Tag(name = "General", description = "Executes general commands."),
      @Tag(name = "Keyspaces", description = "Executes keyspace commands."),
      @Tag(
          name = "Documents",
          description = "Executes document commands against a single collection."),
    },
    components =
        @Components(

            // security schemes
            securitySchemes = {
              @SecurityScheme(
                  securitySchemeName = OpenApiConstants.SecuritySchemes.TOKEN,
                  type = SecuritySchemeType.APIKEY,
                  in = SecuritySchemeIn.HEADER,
                  apiKeyName = HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME,
                  description =
                      """
                      This value depends on the upstream backend. In the case of
                      an Apache Cassandra cluster follows the format
                      `Cassandra:Base64(username):Base64(password)`. For example,
                      assuming a username of `cassandra` and password of
                      `cassandra` the `Token` header would be set to
                      `Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh`.
                      """),
            },

            // reusable parameters
            parameters = {
              @Parameter(
                  in = ParameterIn.PATH,
                  name = "keyspace",
                  required = true,
                  schema =
                      @Schema(
                          implementation = String.class,
                          pattern = "[a-zA-Z][a-zA-Z0-9_]*",
                          maxLength = 48),
                  description = "The keyspace where the collection is located.",
                  example = "cycling"),
              @Parameter(
                  in = ParameterIn.PATH,
                  name = "collection",
                  required = true,
                  schema =
                      @Schema(
                          implementation = String.class,
                          pattern = "[a-zA-Z][a-zA-Z0-9_]*",
                          maxLength = 48),
                  description = "The name of the collection.",
                  example = "events")
            },

            // reusable examples
            examples = {
              @ExampleObject(
                  name = "findOne",
                  summary = "`findOne` command",
                  value =
                      """
                      {
                        "findOne": {
                            "filter": {"location": "London", "race.competitors" : {"$eq" : 100}},
                            "projection": {"_id":0, "location":1, "race.start_date":1, "tags":1},
                            "sort" : {"race.start_date" : 1}
                        }
                      }
                      """),
              @ExampleObject(
                  name = "findOneVectorSearch",
                  summary = "`findOne` command with vector search",
                  value =
                      """
                    {
                      "findOne": {
                          "filter": {"location": "London", "race.competitors" : {"$eq" : 100}},
                          "projection": {"_id":0, "location":1, "race.start_date":1, "tags":1},
                          "sort" : {"$vector" : [0.25,0.25,0.25,0.25,0.25]},
                          "options": {"includeSimilarity" : true}
                      }
                    }
                    """),
              @ExampleObject(
                  name = "countDocuments",
                  summary = "`countDocuments` command",
                  value =
                      """
                      {
                        "countDocuments": {
                            "filter": {"location": "London", "race.competitors" : {"$eq" : 100}}
                        }
                      }
                    """),
              @ExampleObject(
                  name = "find",
                  summary = "`find` command",
                  value =
                      """
                      {
                        "find": {
                             "filter": {"location": "London", "race.competitors" : {"$eq" : 100}},
                             "projection": {"tags":0},
                             "sort" : {"location" : 1},
                             "options": {"limit" : 1000}
                        }
                      }
                      """),
              @ExampleObject(
                  name = "findVectorSearch",
                  summary = "`find` command with vector search",
                  value =
                      """
                      {
                        "find": {
                            "filter": {"location": "London", "race.competitors" : {"$eq" : 100}},
                            "projection": {"tags":0},
                            "sort" : {"$vector" : [0.25,0.25,0.25,0.25,0.25]},
                            "options": {"limit" : 1000, "includeSimilarity" : true}
                        }
                      }
                    """),
              @ExampleObject(
                  name = "findOneAndUpdate",
                  summary = "`findOneAndUpdate` command",
                  value =
                      """
                      {
                        "findOneAndUpdate": {
                            "filter": {"location": "London"},
                            "sort" : {"location" : 1},
                            "update": {
                                "$set": {"location": "New York"},
                                "$inc": {"count": 3}
                            },
                            "projection": {"count": 1,"location": 1,"race": 1},
                            "options" : {
                                "returnDocument" : "before",
                                "upsert" : true
                            }
                        }
                      }
                      """),
              @ExampleObject(
                  name = "findOneAndDelete",
                  summary = "`findOneAndDelete` command",
                  value =
                      """
                      {
                        "findOneAndDelete": {
                            "filter": {"location": "London"},
                            "sort" : {"race.start_date" : 1},
                            "projection" : {"location": 1}
                        }
                      }
                      """),
              @ExampleObject(
                  name = "findOneAndReplace",
                  summary = "`findOneAndReplace` command",
                  value =
                      """
                      {
                        "findOneAndReplace": {
                            "filter": {"location": "London"},
                            "sort" : {"race.start_date" : 1},
                            "replacement": {
                                "location": "New York",
                                "count": 3
                            },
                            "options" : {
                                "returnDocument" : "before",
                                "upsert" : true
                            },
                            "projection" : {"location": 1}
                        }
                      }
                      """),
              @ExampleObject(
                  name = "estimatedDocumentCount",
                  summary = "`estimatedDocumentCount` command",
                  value =
                      """
                      {
                        "estimatedDocumentCount": {}
                      }
                      """),
              @ExampleObject(
                  name = "updateOne",
                  summary = "`updateOne` command",
                  value =
                      """
                      {
                        "updateOne": {
                            "filter": {"location": "London"},
                            "update": {
                                "$set": {"location": "New York"},
                                "$push": {"tags": "marathon"}
                            },
                            "sort" :  {"race.start_date" : 1},
                            "options" : {
                                "upsert" : true
                            }
                        }
                      }
                      """),
              @ExampleObject(
                  name = "updateMany",
                  summary = "`updateMany` command",
                  value =
                      """
                      {
                        "updateMany": {
                            "filter": {"location": "London"},
                            "update": {
                                "$set": {"location": "New York"},
                                "$push": {"tags": "marathon"}
                            },
                            "options" : {
                                "upsert" : true
                            }
                        }
                      }
                      """),
              @ExampleObject(
                  name = "deleteOne",
                  summary = "`deleteOne` command",
                  value =
                      """
                      {
                        "deleteOne": {
                            "filter": {"_id": "1"},
                            "sort" : {"race.start_date" : 1}
                        }
                      }
                      """),
              @ExampleObject(
                  name = "deleteMany",
                  summary = "`deleteMany` command",
                  value =
                      """
                      {
                        "deleteMany": {
                            "filter": {"location": "London"}
                        }
                      }
                      """),
              @ExampleObject(
                  name = "insertOne",
                  summary = "`insertOne` command",
                  value =
                      """
                      {
                        "insertOne": {
                            "document": {
                                "_id": "1",
                                "location": "London",
                                "race": {
                                    "competitors": 100,
                                    "start_date": "2022-08-15"
                                },
                                "tags": [ ],
                                "$vector" : [0.25,0.25,0.25,0.25,0.25]
                            }
                        }
                      }
                      """),
              @ExampleObject(
                  name = "insertMany",
                  summary = "`insertMany` command",
                  value =
                      """
                      {
                        "insertMany": {
                            "documents": [
                                {
                                  "_id": "1",
                                  "location": "London",
                                  "race": {
                                      "competitors": 100,
                                      "start_date": "2022-08-15"
                                  },
                                  "tags" : [ "eu" ],
                                  "$vector" : [0.35,0.35,0.35,0.35,0.35]
                                },
                                {
                                  "_id": "2",
                                  "location": "New York",
                                  "race": {
                                      "competitors": 150,
                                      "start_date": "2022-09-15"
                                  },
                                  "tags": [ "us" ],
                                  "$vector" : [0.45,0.45,0.45,0.45,0.45]
                                }
                            ],
                            "options": {
                                "ordered": true
                            }
                        }
                      }
                      """),
              @ExampleObject(
                  name = "createKeyspace",
                  summary = "`CreateKeyspace` command",
                  value =
                      """
                      {
                        "createKeyspace": {
                            "name": "cycling"
                        }
                      }
                      """),
              @ExampleObject(
                  name = "createKeyspaceWithReplication",
                  summary = "`CreateKeyspace` command with replication",
                  value =
                      """
                      {
                        "createKeyspace": {
                            "name": "cycling",
                            "options": {
                                "replication": {
                                    "class": "SimpleStrategy",
                                    "replication_factor": 3
                                }
                            }
                        }
                      }
                      """),
              @ExampleObject(
                  name = "findKeyspaces",
                  summary = "`FindKeyspaces` command",
                  value =
                      """
                      {
                        "findKeyspaces": {}
                      }
                      """),
              @ExampleObject(
                  name = "dropKeyspace",
                  summary = "`DropKeyspace` command",
                  value =
                      """
                      {
                        "dropKeyspace": {
                            "name": "cycling"
                        }
                      }
                      """),
              @ExampleObject(
                  name = "createCollection",
                  summary = "`CreateCollection` command",
                  value =
                      """
                      {
                        "createCollection": {
                            "name": "events"
                        }
                      }
                      """),
              @ExampleObject(
                  name = "createCollectionLexical",
                  summary = "`CreateCollection` command with lexical indexing ($lexical) enabled",
                  value =
                      """
                                {
                                  "createCollection": {
                                      "name": "events",
                                      "options": {
                                          "lexical": {
                                              "enabled": true,
                                              "analyzer": "standard"
                                          }
                                      }
                                  }
                                }
                                """),
              @ExampleObject(
                  name = "createCollectionReranking",
                  summary = "`CreateCollection` command with reranking model enabled",
                  value =
                      """
                            {
                              "createCollection": {
                                  "name": "events",
                                  "options": {
                                      "rerank": {
                                          "enabled": true,
                                          "service": {
                                              "provider": "nvidia",
                                              "modelName": "nvidia/llama-3.2-nv-rerankqa-1b-v2"
                                          }
                                      }
                                  }
                              }
                            }
                            """),
              @ExampleObject(
                  name = "createCollectionVectorSearch",
                  summary = "`CreateCollection` command with vector search",
                  value =
                      """
                      {
                        "createCollection": {
                            "name": "events",
                            "options": {
                                "vector": {
                                    "dimension": 5,
                                    "metric": "cosine"
                                }
                            }
                        }
                      }
                      """),
              @ExampleObject(
                  name = "findCollections",
                  summary = "`FindCollections` command",
                  value =
                      """
                      {
                        "findCollections": {}
                      }
                      """),
              @ExampleObject(
                  name = "deleteCollection",
                  summary = "`DeleteCollection` command",
                  value =
                      """
                      {
                        "deleteCollection": {
                            "name": "events"
                        }
                      }
                      """),
              @ExampleObject(
                  name = "resultCount",
                  summary = "`countDocuments` command result",
                  value =
                      """
                      {
                        "status": {
                            "count": 2
                        }
                      }
                      """),
              @ExampleObject(
                  name = "resultFind",
                  summary = "`find` command result",
                  value =
                      """
                      {
                        "data": {
                            "documents": [
                                {
                                  "_id": "1",
                                  "location": "London",
                                  "race": {
                                      "competitors": 100,
                                      "start_date": "2022-08-15"
                                  },
                                  "tags": [ "eu" ]
                                },
                                {
                                  "_id": "2",
                                  "location": "Barcelona",
                                  "race": {
                                      "competitors": 125,
                                      "start_date": "2022-09-26"
                                  },
                                  "tags": [ "us" ]
                                }
                            ]
                        }
                      }
                      """),
              @ExampleObject(
                  name = "resultFindOne",
                  summary = "`findOne` command result",
                  value =
                      """
                      {
                        "data": {
                            "document": {
                                "location": "London",
                                "race": {
                                    "start_date": "2022-08-15"
                                },
                                "tags": [ "eu" ]
                            }
                        }
                      }
                      """),
              @ExampleObject(
                  name = "resultFindOneAndUpdate",
                  summary = "`findOneAndUpdate` command result",
                  value =
                      """
                      {
                        "data": {
                            "document": {
                              "_id": "1",
                              "location": "New York",
                              "race": {
                                  "competitors": 100,
                                  "start_date": "2022-08-15"
                              },
                              "count": 3
                            }
                        },
                        "status": {
                            "matchedCount": 1,
                            "modifiedCount": 1
                        }
                      }
                      """),
              @ExampleObject(
                  name = "resultFindOneAndReplace",
                  summary = "`findOneAndReplace` command result",
                  value =
                      """
                      {
                        "data": {
                            "document": {
                              "_id": "1",
                              "location": "New York",
                              "count": 3
                            }
                        },
                        "status": {
                            "matchedCount": 1,
                            "modifiedCount": 1
                        }
                     }
                     """),
              @ExampleObject(
                  name = "resultFindOneAndDelete",
                  summary = "`findOneAndDetele` command result",
                  value =
                      """
                      {
                        "data": {
                            "document": {
                                "_id": "1",
                                "location": "New York",
                                "count": 3
                            }
                        },
                        "status": {
                            "deletedCount": 1
                        }
                      }
                      """),
              @ExampleObject(
                  name = "resultFindOneAndUpdateUpsert",
                  summary = "`findOneAndUpdate` command with upsert result",
                  value =
                      """
                      {
                        "data": {
                            "document": {
                                "_id": "1",
                                "location": "New York",
                                "count": 3
                            }
                        },
                        "status": {
                            "upsertedId": "1",
                            "matchedCount": 0,
                            "modifiedCount": 1
                        }
                      }
                      """),
              @ExampleObject(
                  name = "resultUpdateOne",
                  summary = "`updateOne` command result",
                  value =
                      """
                      {
                        "status": {
                            "matchedCount": 1,
                            "modifiedCount": 1
                        }
                      }
                      """),
              @ExampleObject(
                  name = "resultUpdateOneUpsert",
                  summary = "`updateOne` command with upsert result",
                  value =
                      """
                      {
                        "status": {
                            "upsertedId": "1",
                            "matchedCount": 0,
                            "modifiedCount": 1
                        }
                      }
                      """),
              @ExampleObject(
                  name = "resultUpdateMany",
                  summary = "`updateMany` command result",
                  value =
                      """
                      {
                        "status": {
                            "matchedCount": 20,
                            "modifiedCount": 20,
                            "moreData": true
                        }
                      }
                  """),
              @ExampleObject(
                  name = "resultUpdateManyUpsert",
                  summary = "`updateMany` command with upsert result",
                  value =
                      """
                      {
                        "status": {
                            "upsertedId": "1",
                            "matchedCount": 0,
                            "modifiedCount": 1
                        }
                      }
                  """),
              @ExampleObject(
                  name = "resultInsert",
                  summary = "`insertOne` & `insertMany` command result",
                  value =
                      """
                      {
                        "status": {
                            "insertedIds": ["1", "2"]
                        }
                      }
                      """),
              @ExampleObject(
                  name = "resultDeleteOne",
                  summary = "`deleteOne` command result",
                  value =
                      """
                      {
                        "status": {
                            "deletedCount": 1
                        }
                      }
                      """),
              @ExampleObject(
                  name = "resultDeleteMany",
                  summary = "`deleteMany` command result",
                  value =
                      """
                      {
                        "status": {
                            "deletedCount": 2,
                            "moreData" : true
                        }
                      }
                      """),
              @ExampleObject(
                  name = "resultFindKeyspaces",
                  summary = "`findKeyspaces` command result",
                  value =
                      """
                      {
                        "status": {
                            "keyspaces": [
                                "cycling"
                            ]
                        }
                      }
                      """),
              @ExampleObject(
                  name = "resultFindCollections",
                  summary = "`findCollections` command result",
                  value =
                      """
                      {
                        "status": {
                            "collections": [ "events" ]
                        }
                      }
                      """),
              @ExampleObject(
                  name = "resultError",
                  summary = "Error result",
                  value =
                      """
                      {
                        "errors": [
                            {
                                "message": "The command failed because of some specific reason."
                            }
                        ]
                      }
                      """),
              @ExampleObject(
                  name = "resultCreate",
                  summary = "Create result",
                  value =
                      """
                      {
                        "status": {
                            "ok": 1
                        }
                      }
                      """),

              // Table examples
              @ExampleObject(
                  name = "createTableWithSingleKey",
                  summary = "`createTable` command with single partition key",
                  value =
                      """
                                    {
                                         "createTable": {
                                             "name": "person",
                                             "definition": {
                                                 "columns": {
                                                     "id": "text",
                                                     "age": "int",
                                                     "name": "text",
                                                     "city": "text"
                                                 },
                                                 "primaryKey": "id"
                                             },
                                             "options" : {
                                                 "ifNotExists" : true
                                             }
                                         }
                                     }
                                """),
              @ExampleObject(
                  name = "createTableWithMultipleKeys",
                  summary = "`createTable` command with multiple primary key",
                  value =
                      """
                                        {
                                          "createTable": {
                                              "name": "employee",
                                              "definition": {
                                                  "columns": {
                                                      "id": "text",
                                                      "age": {
                                                          "type": "int"
                                                      },
                                                      "name": {
                                                          "type": "text"
                                                      },
                                                      "city": {
                                                          "type": "text"
                                                      }
                                                  },
                                                  "primaryKey": {
                                                      "partitionBy": [
                                                          "id"
                                                      ],
                                                      "partitionSort": {
                                                          "name" : 1, "age" : -1
                                                      }
                                                  }
                                              }
                                          }
                                        }
                                    """),
              @ExampleObject(
                  name = "dropTable",
                  summary = "`dropTable` request",
                  value =
                      """
                                    {
                                         "dropTable": {
                                             "name": "person",
                                             "options" : {
                                                 "ifExists" : true
                                             }
                                         }
                                     }
                                      """),
              @ExampleObject(
                  name = "alterTableAddColumns",
                  summary = "`alterTable` add columns request",
                  value =
                      """
                                        {
                                              "alterTable": {
                                                  "operation": {
                                                      "add": {
                                                          "columns": {
                                                              "new_col_1": "text",
                                                              "new_col_2": {
                                                                  "type": "map",
                                                                  "keyType": "text",
                                                                  "valueType": "text"
                                                              },
                                                              "content": {
                                                                  "type": "vector",
                                                                  "dimension": 1024,
                                                                  "service": {
                                                                      "provider": "nvidia",
                                                                      "modelName": "NV-Embed-QA"
                                                                  }
                                                              },
                                                              "embedding": {
                                                                  "type": "vector",
                                                                  "dimension": 1024
                                                              }
                                                          }
                                                      }
                                                  }
                                              }
                                          }
                                        """),
              @ExampleObject(
                  name = "alterTableDropColumns",
                  summary = "`alterTable` drop columns request",
                  value =
                      """
                                            {
                                                "alterTable": {
                                                    "operation": {
                                                        "drop": {
                                                            "columns": ["content"]
                                                        }
                                                    }
                                                }
                                            }
                                            """),
              @ExampleObject(
                  name = "alterTableAddVectorize",
                  summary = "`alterTable` add vectorize config request",
                  value =
                      """
                                            {
                                                 "alterTable": {
                                                     "operation": {
                                                         "addVectorize": {
                                                             "columns": {
                                                                 "embedding": {
                                                                     "provider": "mistral",
                                                                     "modelName": "mistral-embed"
                                                                 }
                                                             }
                                                         }
                                                     }
                                                 }
                                             }
                                            """),
              @ExampleObject(
                  name = "alterTableDropVectorize",
                  summary = "`alterTable` drop vectorize config request",
                  value =
                      """
                                            {
                                                  "alterTable": {
                                                      "operation": {
                                                          "dropVectorize": {
                                                              "columns": ["embedding"]
                                                          }
                                                      }
                                                  }
                                              }
                                            """),
              @ExampleObject(
                  name = "createIndex",
                  summary = "`createIndex` for non vector columns, in tables api",
                  value =
                      """
                                    {
                                          "createIndex": {
                                              "name": "name_2_idx",
                                              "definition": {
                                                  "column": "name",
                                                  "options": {
                                                      "caseSensitive": false,
                                                      "normalize": true,
                                                      "ascii": true
                                                  }
                                              },
                                              "options" : {
                                                  "ifNotExists" : true
                                              }
                                          }
                                      }
                                      """),
              @ExampleObject(
                  name = "createVectorIndex",
                  summary = "`createVectorIndex` for vector columns, in tables api",
                  value =
                      """
                                            {
                                                   "createVectorIndex": {
                                                       "name": "embeddings_idx",
                                                       "definition": {
                                                           "column": "embeddings"
                                                       },
                                                       "options" : {
                                                           "ifNotExists" : true
                                                       }
                                                   }
                                               }
                                          """),
              @ExampleObject(
                  name = "dropIndex",
                  summary = "`dropIndex` to drop an index, in tables api",
                  value =
                      """
                                              {
                                                  "dropIndex": {
                                                      "name" : "city_index",
                                                      "options" : {
                                                          "ifExists" : false
                                                      }
                                                  }
                                              }
                                          """),
              @ExampleObject(
                  name = "resultDdl",
                  summary = "Create table result",
                  value =
                      """
                                              {
                                                "status": {
                                                    "ok": 1
                                                }
                                              }
                                              """),
              @ExampleObject(
                  name = "listTables",
                  summary = "`listTables` lists all tables in a keyspace",
                  value =
                      """
                                              {
                                                 "listTables": {
                                                     "options" : {
                                                         "explain" : true
                                                     }
                                                 }
                                             }
                                          """),
              @ExampleObject(
                  name = "listTablesResponse",
                  summary = "`listTables` response",
                  value =
                      """
                                            {
                                                "status": {
                                                    "tables": [
                                                        {
                                                          "name": "employee",
                                                          "definition": {
                                                              "columns": {
                                                                  "id": {
                                                                      "type": "text"
                                                                  },
                                                                  "name": {
                                                                      "type": "text"
                                                                  },
                                                                  "age": {
                                                                      "type": "int"
                                                                  },
                                                                  "city": {
                                                                      "type": "text"
                                                                  }
                                                              },
                                                              "primaryKey": {
                                                                  "partitionBy": [
                                                                      "id"
                                                                  ],
                                                                  "partitionSort": {
                                                                      "name": 1,
                                                                      "age": -1
                                                                  }
                                                              }
                                                          }
                                                      },
                                                      {
                                                            "name": "person",
                                                            "definition": {
                                                                "columns": {
                                                                    "id": {
                                                                        "type": "text"
                                                                    },
                                                                    "age": {
                                                                        "type": "int"
                                                                    },
                                                                    "city": {
                                                                        "type": "text"
                                                                    },
                                                                    "name": {
                                                                        "type": "text"
                                                                    }
                                                                },
                                                                "primaryKey": {
                                                                    "partitionBy": [
                                                                        "id"
                                                                    ],
                                                                    "partitionSort": {}
                                                                }
                                                            }
                                                        }
                                                    ]
                                                }
                                            }
                                          """),
              @ExampleObject(
                  name = "listIndexes",
                  summary = "`listIndexes` to list all indexes for a table, in tables api",
                  value =
                      """
                                     {
                                         "listIndexes": {
                                             "options" : {
                                                 "explain" : true
                                             }
                                         }
                                     }
                                     """),
              @ExampleObject(
                  name = "listIndexesResponse",
                  summary = "`listIndexes` response, in tables api",
                  value =
                      """
                                        {
                                             "status": {
                                                 "indexes": [
                                                     {
                                                         "name": "name_2_idx",
                                                         "definition": {
                                                             "column": "name",
                                                             "options": {
                                                                 "ascii": true,
                                                                 "caseSensitive": false,
                                                                 "normalize": true
                                                             }
                                                         }
                                                     }
                                                 ]
                                             }
                                         }
                                     """),
              @ExampleObject(
                  name = "insertManyTables",
                  summary = "`insertMany` for tables api",
                  value =
                      """
                                        {
                                           "insertMany": {
                                               "documents": [
                                                   {
                                                       "id": "user1",
                                                       "age": 19,
                                                       "name": "Charlie",
                                                       "city": "New York"
                                                   },
                                                   {
                                                       "id": "user2",
                                                       "age": 54,
                                                       "name": "Julia",
                                                       "city": "Houston"
                                                   }
                                                  ]
                                                }
                                            }
                                        """),
              @ExampleObject(
                  name = "insertOneTables",
                  summary = "`insertOne` for tables api",
                  value =
                      """
                                                      {
                                                         "insertOne": {
                                                            "document": {
                                                                     "id": "user1",
                                                                     "age": 19,
                                                                     "name": "Charlie",
                                                                     "city": "New York"
                                                                 }
                                                              }
                                                          }
                                                      """),
              @ExampleObject(
                  name = "insertManyTablesResponse",
                  summary = "`insertMany` response for tables api",
                  value =
                      """
                                            {
                                                     "status": {
                                                         "primaryKeySchema": {
                                                             "id": {
                                                                 "type": "text"
                                                             }
                                                         },
                                                         "insertedIds": [
                                                             [
                                                                 "user1"
                                                             ],
                                                             [
                                                                 "user2"
                                                             ]
                                                         ]
                                                     }
                                                 }
                                            """),
              @ExampleObject(
                  name = "findTables",
                  summary = "`find` for tables api",
                  value =
                      """
                                            {
                                                     "find": {
                                                         "filter" : {"name" : "Charlie"},
                                                         "projection" : {
                                                             "id" : 1,
                                                             "name" : 1,
                                                             "age" : 1
                                                         },
                                                         "sort" : {
                                                             "age" : 1
                                                         },
                                                         "options" : {
                                                             "limit" : 2
                                                         }
                                                     }
                                                 }
                                            """),
              @ExampleObject(
                  name = "findTablesResponse",
                  summary = "`find` response for tables api",
                  value =
                      """
                                            {
                                                      "data": {
                                                          "documents": [
                                                              {
                                                                  "id": "user1",
                                                                  "name": "Charlie",
                                                                  "age": 19
                                                              },
                                                              {
                                                                  "id": "user12",
                                                                  "name": "Charlie",
                                                                  "age": 33
                                                              }
                                                          ],
                                                          "nextPageState": null
                                                      },
                                                      "status": {
                                                          "warnings": [
                                                              {
                                                                  "errorCode": "IN_MEMORY_SORTING_DUE_TO_NON_PARTITION_SORTING",
                                                                  "message": "The command used columns in the sort clause that are not part of the partition sorting, and so the query was sorted in memory.\\n      \\nThe table default_keyspace.person has the partition sorting columns: [None].\\nThe command sorted on the columns: age.\\n\\nThe command was executed using in memory sorting rather than taking advantage of the partition sorting on disk. This can have performance implications on large tables.\\n\\nSee documentation at XXXX for best practices for sorting.",
                                                                  "family": "REQUEST",
                                                                  "scope": "WARNING",
                                                                  "title": "Sorting by non partition sorting columns",
                                                                  "id": "806b62a0-6d60-464b-ab8a-31f1aac9e46e"
                                                              }
                                                          ],
                                                          "sortedRowCount": 4,
                                                          "projectionSchema": {
                                                              "id": {
                                                                  "type": "text"
                                                              },
                                                              "name": {
                                                                  "type": "text"
                                                              },
                                                              "age": {
                                                                  "type": "int"
                                                              }
                                                          }
                                                      }
                                                  }
                                            """),
              @ExampleObject(
                  name = "findEmbeddingProviders",
                  summary = "`findEmbeddingProviders` command result",
                  value =
                      """
                      {
                        "status": {
                            "embeddingProviders": {
                                "openai": {
                                    "displayName": "OpenAI",
                                    "url": "https://api.openai.com/v1/",
                                    "supportedAuthentication": {
                                        "HEADER": {
                                            "enabled": true,
                                            "tokens": [
                                                {
                                                    "forwarded": "Authorization",
                                                    "accepted": "x-embedding-api-key"
                                                }
                                            ]
                                        },
                                        "SHARED_SECRET": {
                                            "enabled": false,
                                            "tokens": [
                                                {
                                                    "forwarded": "Authorization",
                                                    "accepted": "providerKey"
                                                }
                                            ]
                                        },
                                        "NONE": {
                                            "enabled": false,
                                            "tokens": []
                                        }
                                    },
                                    "parameters": [
                                        {
                                            "name": "organizationId",
                                            "type": "STRING",
                                            "defaultValue": null,
                                            "displayName": "Organization ID",
                                            "help": "Optional, OpenAI Organization ID. If provided passed as `OpenAI-Organization` header.",
                                            "hint": "Add an (optional) organization ID",
                                            "validation": {},
                                            "required": false
                                        },
                                        {
                                            "name": "projectId",
                                            "type": "STRING",
                                            "defaultValue": null,
                                            "displayName": "Project ID",
                                            "help": "Optional, OpenAI Project ID. If provided passed as `OpenAI-Project` header.",
                                            "hint": "Add an (optional) project ID",
                                            "validation": {},
                                            "required": false
                                        }
                                    ],
                                    "models": [
                                        {
                                            "name": "text-embedding-3-small",
                                            "vectorDimension": null,
                                            "parameters": [
                                                {
                                                    "name": "vectorDimension",
                                                    "type": "number",
                                                    "required": true,
                                                    "defaultValue": "512",
                                                    "validation": {
                                                        "numericRange": [
                                                            2,
                                                            1536
                                                        ]
                                                    },
                                                    "help": "Vector dimension to use in the database and when calling OpenAI."
                                                }
                                            ]
                                        },
                                        {
                                            "name": "text-embedding-3-large",
                                            "vectorDimension": null,
                                            "parameters": [
                                                {
                                                    "name": "vectorDimension",
                                                    "type": "number",
                                                    "required": true,
                                                    "defaultValue": "1024",
                                                    "validation": {
                                                        "numericRange": [
                                                            256,
                                                            3072
                                                        ]
                                                    },
                                                    "help": "Vector dimension to use in the database and when calling OpenAI."
                                                }
                                            ]
                                        },
                                        {
                                            "name": "text-embedding-ada-002",
                                            "vectorDimension": 1536,
                                            "parameters": []
                                        }
                                    ]
                                }
                            }
                        }
                      }
                      """)
            }))
public class StargateJsonApi extends Application {
  @Produces
  public String sourceApi() {
    return "rest";
  }
}
