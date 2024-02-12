package io.stargate.sgv2.jsonapi;

import io.quarkus.arc.lookup.LookupIfProperty;
import io.quarkus.vertx.http.runtime.AuthConfig;
import io.stargate.sgv2.api.common.grpc.SourceApiQualifier;
import io.stargate.sgv2.jsonapi.api.request.EmbeddingApiKeyResolver;
import io.stargate.sgv2.jsonapi.api.request.HeaderBasedEmbeddingApiKeyResolver;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.config.constants.OpenApiConstants;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Application;
import org.checkerframework.checker.units.qual.A;
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
      @Tag(name = "Namespaces", description = "Executes namespace commands."),
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
                  apiKeyName = HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME)
            },

            // reusable parameters
            parameters = {
              @Parameter(
                  in = ParameterIn.PATH,
                  name = "namespace",
                  required = true,
                  schema =
                      @Schema(
                          implementation = String.class,
                          pattern = "[a-zA-Z][a-zA-Z0-9_]*",
                          maxLength = 48),
                  description = "The namespace where the collection is located.",
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
                             "options": {"limit" : 1000, "pageState" : "Next page state got from previous page call"}
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
                  name = "createNamespace",
                  summary = "`CreateNamespace` command",
                  value =
                      """
                        {
                            "createNamespace": {
                              "name": "cycling"
                            }
                        }
                      """),
              @ExampleObject(
                  name = "createEmbeddingService",
                  summary = "`createEmbeddingService` command",
                  value =
                      """
                          {
                            "createEmbeddingService": {
                              "name": "open_ai_service_name",
                              "apiProvider" : "openai",
                              "apiKey" : "token",
                              "baseUrl" : "https://api.openai.com/v1/"
                            }
                          }
                      """),
              @ExampleObject(
                  name = "createNamespaceWithReplication",
                  summary = "`CreateNamespace` command with replication",
                  value =
                      """
                        {
                            "createNamespace": {
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
                  name = "findNamespaces",
                  summary = "`FindNamespaces` command",
                  value =
                      """
                        {
                            "findNamespaces": {
                            }
                        }
                      """),
              @ExampleObject(
                  name = "dropNamespace",
                  summary = "`DropNamespace` command",
                  value =
                      """
                        {
                            "dropNamespace": {
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
                  name = "createCollectionVectorSearch",
                  summary = "`CreateCollection` command with vector search",
                  value =
                      """
                        {
                            "createCollection": {
                              "name": "events_vector",
                              "options": {
                                "vector": {
                                  "dimension": 2,
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
                            "findCollections": {
                            }
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
                          ],
                          "nextPageState": "jA8qg0AitZ8q28568GybNQ=="
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
                  name = "resultFindNamespaces",
                  summary = "`findNamespaces` command result",
                  value =
                      """
                                {
                                  "status": {
                                    "namespaces": [
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
            }))
public class StargateJsonApi extends Application {

  @Produces
  @SourceApiQualifier
  public String sourceApi() {
    return "rest";
  }

    @Produces
    @ApplicationScoped
    EmbeddingApiKeyResolver headerTokenResolver() {
        return new HeaderBasedEmbeddingApiKeyResolver("embedding-api-key");
    }
}
