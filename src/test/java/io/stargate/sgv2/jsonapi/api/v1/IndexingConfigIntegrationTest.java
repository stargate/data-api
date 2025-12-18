package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsError;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsFindSuccess;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.*;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class IndexingConfigIntegrationTest extends AbstractCollectionIntegrationTestBase {

  private static final String denyOneIndexingCollection = "deny_one_indexing_collection";

  private static final String denyManyIndexingCollection = "deny_many_indexing_collection";

  private static final String denyAllIndexingCollection = "deny_all_indexing_collection";

  private static final String allowOneIndexingCollection = "allow_one_indexing_collection";

  private static final String allowManyIndexingCollection = "allow_many_indexing_collection";

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  @Order(1)
  class CreateCollectionAndData {

    @Test
    @Order(1)
    public void createCollectionAndData() {
      String insertData =
          """
            {
              "_id": "1",
              "name": "aaron",
              "$vector": [
                0.25,
                0.25,
                0.25,
                0.25,
                0.25
              ],
              "address": {
                "street": "1 banana street",
                "city": "monkey town"
              },
              "contact": [
                {
                  "phone": "123",
                  "email": "123@gmail.com"
                },
                "use phone please"
              ],
              "pricing": {
                "price.usd": 1,
                "pricing.price&jpy": 1,
                "pricing.price&.aud": 1
              },
              "metadata": {
                "app.kubernetes.io/name": "test"
              }
            }
            """;
      String denyOneIndexingCollectionSetting =
              """
              {
                "name": "%s",
                "options" : {
                  "vector" : {
                    "size" : 5,
                    "function" : "cosine"
                  },
                  "indexing" : {
                    "deny" : ["address.city"]
                  }
                }
              }
              """
              .formatted(denyOneIndexingCollection);
      String denyManyIndexingCollectionSetting =
              """
              {
                "name": "%s",
                "options" : {
                  "vector" : {
                    "size" : 5,
                    "function" : "cosine"
                  },
                  "indexing" : {
                    "deny" : ["name", "address", "contact.email"]
                  }
                }
              }
              """
              .formatted(denyManyIndexingCollection);
      String denyAllIndexingCollectionSetting =
              """
              {
                "name": "%s",
                "options" : {
                  "vector" : {
                    "size" : 5,
                    "function" : "cosine"
                  },
                  "indexing" : {
                    "deny" : ["*"]
                  }
                }
              }
              """
              .formatted(denyAllIndexingCollection);
      String allowOneIndexingCollectionSetting =
              """
              {
                "name": "%s",
                "options" : {
                  "vector" : {
                    "size" : 5,
                    "function" : "cosine"
                  },
                  "indexing" : {
                    "allow" : ["name"]
                  }
                }
              }
              """
              .formatted(allowOneIndexingCollection);
      String allowManyIndexingCollectionSetting =
              """
              {
                "name": "%s",
                "options" : {
                  "vector" : {
                    "size" : 5,
                    "function" : "cosine"
                  },
                  "indexing" : {
                    "allow" : ["name", "address.city", "pricing.price&.usd", "pricing.price&&jpy", "metadata.app&.kubernetes&.io/name"]
                  }
                }
              }
              """
              .formatted(allowManyIndexingCollection);

      createComplexCollection(denyOneIndexingCollectionSetting);
      createComplexCollection(denyManyIndexingCollectionSetting);
      createComplexCollection(denyAllIndexingCollectionSetting);
      createComplexCollection(allowOneIndexingCollectionSetting);
      createComplexCollection(allowManyIndexingCollectionSetting);

      insertDoc(denyOneIndexingCollection, insertData);
      insertDoc(denyManyIndexingCollection, insertData);
      insertDoc(denyAllIndexingCollection, insertData);
      insertDoc(allowOneIndexingCollection, insertData);
      insertDoc(allowManyIndexingCollection, insertData);
    }
  }

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  @Order(2)
  class IndexingConfig {

    @Test
    public void filterFieldInDenyOne() {
      // explicitly deny "address.city", implicitly allow "_id", "name", "address.street"
      givenHeadersPostJsonThenOk(
              keyspaceName,
              denyOneIndexingCollection,
              """
              {
                "find": {
                  "filter": {"address.city": "monkey town"}
                }
              }
                """)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("FILTER_PATH_UNINDEXED"))
          .body("errors[0].exceptionClass", is("FilterException"))
          .body(
              "errors[0].message",
              containsString("Collection path 'address.city' is not indexed: cannot filter"));
    }

    @Test
    public void filterVectorFieldInDenyAll() {
      // explicitly deny "address.city", implicitly allow "_id", "name", "address.street"
      givenHeadersPostJsonThenOk(
              keyspaceName,
              denyAllIndexingCollection,
              """
            {
              "find": {
                "filter": {"$vector": {"$exists": true}}
              }
            }
            """)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("FILTER_PATH_UNINDEXED"))
          .body("errors[0].exceptionClass", is("FilterException"))
          .body(
              "errors[0].message",
              containsString("Collection path '$vector' is not indexed: cannot filter"));
    }

    @Test
    public void filterFieldNotInDenyOne() {
      // explicitly deny "address.city", implicitly allow "_id", "name", "address.street"
      givenHeadersAndJson(
              """
              {
                "find": {
                  "filter": {"name": "aaron"}
                }
              }
              """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, denyOneIndexingCollection)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(1));
      // deny "address.city", only this as a string, not "address" as an object
      givenHeadersAndJson(
              """
                  {
                    "find": {
                      "filter": {
                        "address": {
                          "$eq": {
                            "street": "1 banana street"
                          }
                        }
                      }
                    }
                  }
                    """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, denyOneIndexingCollection)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(1));
      givenHeadersAndJson(
              """
                      {
                        "find": {
                          "filter": {
                            "address": {
                              "$eq": {
                                "street": "1 banana street",
                                "city": "monkey town"
                              }
                            }
                          }
                        }
                      }
                        """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, denyOneIndexingCollection)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("FILTER_PATH_UNINDEXED"))
          .body("errors[0].exceptionClass", is("FilterException"))
          .body(
              "errors[0].message",
              containsString("Collection path 'address.city' is not indexed: cannot filter"));
    }

    @Test
    public void filterFieldInDenyMany() {
      // explicitly deny "name", "address", implicitly allow "_id"
      // deny "address", "address.city" should also be included
      givenHeadersAndJson(
              """
                  {
                    "find": {
                      "filter": {"address.city": "monkey town"}
                    }
                  }
                    """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, denyManyIndexingCollection)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("FILTER_PATH_UNINDEXED"))
          .body("errors[0].exceptionClass", is("FilterException"))
          .body(
              "errors[0].message",
              containsString("Collection path 'address.city' is not indexed: cannot filter"));
    }

    @Test
    public void filterFieldInDenyAll() {
      givenHeadersAndJson(
              """
                  {
                    "find": {
                      "filter": {"address.city": "monkey town"}
                    }
                  }
                    """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, denyAllIndexingCollection)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("FILTER_PATH_UNINDEXED"))
          .body("errors[0].exceptionClass", is("FilterException"))
          .body(
              "errors[0].message",
              containsString("Collection path 'address.city' is not indexed: cannot filter"));
    }

    @Test
    public void filterIdInDenyAllWithEqAndIn() {
      givenHeadersAndJson(
              """
                  {
                    "find": {
                      "filter": {"_id": "1"}
                    }
                  }
                    """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, denyAllIndexingCollection)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(1));

      givenHeadersAndJson(
              """
                  {
                      "find": {
                          "filter": {
                              "_id": {
                                    "$in": [
                                        "1",
                                        "2"
                                    ]
                                }
                          }
                      }
                  }
                      """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, denyAllIndexingCollection)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(1));
    }

    @Test
    public void filterIdInDenyAllWithoutEqAndIn() {
      givenHeadersAndJson(
              """
                {
                    "find": {
                        "filter": {
                            "_id": {
                                "$nin": [
                                    "1",
                                    "2"
                                ]
                            }
                        }
                    }
                }
                """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, denyAllIndexingCollection)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("FILTER_ID_NOT_INDEXED"))
          .body("errors[0].exceptionClass", is("FilterException"))
          .body(
              "errors[0].message",
              containsString(
                  "Collection field '_id' is never indexed so filtering can only be done using operators '$eq' or '$in', not '$nin'"));
    }

    @Test
    public void filterFieldInAllowOne() {
      // explicitly allow "name", implicitly deny "_id" "address"
      givenHeadersAndJson(
              """
                  {
                    "find": {
                      "filter": {"name": "aaron"}
                    }
                  }
                  """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, allowOneIndexingCollection)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(1));
    }

    @Test
    public void filterFieldNotInAllowOne() {
      // explicitly allow "name", implicitly deny "_id" "address.city" "address.street" "address"
      givenHeadersAndJson(
              """
                      {
                        "find": {
                          "filter": {
                            "address": {
                              "$eq": {
                                "street": "1 banana street",
                                "city": "monkey town"
                              }
                            }
                          }
                        }
                      }
                      """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, allowOneIndexingCollection)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("FILTER_PATH_UNINDEXED"))
          .body("errors[0].exceptionClass", is("FilterException"))
          .body(
              "errors[0].message",
              containsString(
                  "Collection path 'address' is not indexed: cannot filter on that path"));
      givenHeadersAndJson(
              """
                    {
                        "find": {
                            "filter": {
                                "_id": {
                                    "$nin": [
                                        "1",
                                        "2"
                                    ]
                                }
                            }
                        }
                    }
                    """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, allowOneIndexingCollection)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("FILTER_ID_NOT_INDEXED"))
          .body("errors[0].exceptionClass", is("FilterException"))
          .body(
              "errors[0].message",
              containsString(
                  "Collection field '_id' is never indexed so filtering can only be done using operators '$eq' or '$in', not '$nin'"));
      givenHeadersAndJson(
              """
                      {
                        "find": {
                          "filter": {"_id": "1"}
                        }
                      }
                      """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, allowOneIndexingCollection)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(1));
    }

    @Test
    public void filterFieldInAllowMany() {
      // explicitly allow "name" "address.city", implicitly deny "_id" "address.street"
      givenHeadersAndJson(
              """
                      {
                          "find": {
                              "filter": {
                                  "$and": [
                                      {
                                          "$or": [
                                              {
                                                  "address.city": "New York"
                                              },
                                              {
                                                  "address.city": "monkey town"
                                              }
                                          ]
                                      },
                                      {
                                          "$or": [
                                              {
                                                  "name": "Jim"
                                              },
                                              {
                                                  "name": "aaron"
                                              }
                                          ]
                                      }
                                  ]
                              }
                          }
                      }
                      """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, allowManyIndexingCollection)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(1));
    }

    @Test
    public void filterFieldNotInAllowMany() {
      // explicitly allow "name" "address.city", implicitly deny "_id" "address.street" "address"
      // _id is allowed using in
      givenHeadersAndJson(
              """
                {
                  "find": {
                    "filter": {
                      "$and": [
                        {
                          "_id": {
                            "$in": [
                              "1",
                              "2"
                            ]
                          }
                        },
                        {
                          "$or": [
                            {
                              "address.street": "1 banana street"
                            },
                            {
                              "address.street": "2 banana street"
                            }
                          ]
                        }
                      ]
                    }
                  }
                }
                """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, allowManyIndexingCollection)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("FILTER_PATH_UNINDEXED"))
          .body("errors[0].exceptionClass", is("FilterException"))
          .body(
              "errors[0].message",
              containsString("Collection path 'address.street' is not indexed: cannot filter"));
      // allow "address.city", only this as a string, not "address" as an object
      givenHeadersAndJson(
              """
              {
                "find": {
                  "filter": {
                    "address": {
                      "$eq": {
                        "street": "1 banana street",
                        "city": "monkey town"
                      }
                    }
                  }
                }
              }
              """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, allowManyIndexingCollection)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("FILTER_PATH_UNINDEXED"))
          .body("errors[0].exceptionClass", is("FilterException"))
          .body(
              "errors[0].message",
              containsString("Collection path 'address' is not indexed: cannot filter"));
    }

    @Test
    public void incrementalPathInArray() {
      // explicitly deny "address.city", implicitly allow "_id", "name", "address.street" "address"
      // String and array in array - no incremental path, the path is "address" - should be allowed
      // but no data return
      givenHeadersAndJson(
              """
                  {
                    "find": {
                      "filter": {
                        "address": {
                          "$in": [[{"city": "monkey town"}], "abc", ["def"]]
                        }
                      }
                    }
                  }
                  """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, denyOneIndexingCollection)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("FILTER_PATH_UNINDEXED"))
          .body("errors[0].exceptionClass", is("FilterException"))
          .body(
              "errors[0].message",
              containsString(
                  "Collection path 'address.city' is not indexed: cannot filter on that path"));
      // explicitly deny "address.city", implicitly allow "_id", "name", "address.street" "address"
      givenHeadersAndJson(
              """
                      {
                        "find": {
                          "filter": {
                            "address": {
                              "$in": [{"street": "1 banana street"}]
                            }
                          }
                        }
                      }
                      """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, denyOneIndexingCollection)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(1));
      // explicitly deny "address.city", implicitly allow "_id", "name", "address.street" "address"
      // Object (Hashmap) in array - incremental path is "address.city"
      givenHeadersAndJson(
              """
                  {
                    "find": {
                      "filter": {
                        "address": {
                          "$in": [
                            {
                              "city": "monkey town"
                            }
                          ]
                        }
                      }
                    }
                  }
                  """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, denyOneIndexingCollection)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("FILTER_PATH_UNINDEXED"))
          .body("errors[0].exceptionClass", is("FilterException"))
          .body(
              "errors[0].message",
              containsString(
                  "Collection path 'address.city' is not indexed: cannot filter on that path"));
      // explicitly deny "name", "address" "contact.email"
      givenHeadersAndJson(
              """
                  {
                    "find": {
                      "filter": {
                        "contact": {
                          "$in": [
                            [
                              {
                                "phone": "123",
                                "email": "123@gmail.com"
                              }
                            ]
                          ]
                        }
                      }
                    }
                  }
                  """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, denyManyIndexingCollection)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("FILTER_PATH_UNINDEXED"))
          .body("errors[0].exceptionClass", is("FilterException"))
          .body(
              "errors[0].message",
              containsString(
                  "Collection path 'contact.email' is not indexed: cannot filter on that path"));
    }

    @Test
    public void incrementalPathInMap() {
      givenHeadersAndJson(
              """
                  {
                    "find": {
                      "filter": {
                        "address": {
                          "$eq": {
                            "city": {
                              "zipcode": "12345"
                            }
                          }
                        }
                      }
                    }
                  }
                  """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, denyOneIndexingCollection)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("FILTER_PATH_UNINDEXED"))
          .body("errors[0].exceptionClass", is("FilterException"))
          .body(
              "errors[0].message",
              containsString(
                  "Collection path 'address.city' is not indexed: cannot filter on that path."));
    }

    @Test
    public void sortFieldInAllowMany() {
      givenHeadersAndJson(
              """
                  {
                    "find": {
                      "sort": {
                         "$vector" : [0.15, 0.1, 0.1, 0.35, 0.55]
                      }
                    }
                  }
                  """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, denyManyIndexingCollection)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(1));
    }

    @Test
    public void sortFieldNotInAllowMany() {
      // explicitly allow "name" "address.city", implicitly deny "_id" "address.street";
      // (and implicitly allow "$vector" as well)
      givenHeadersAndJson(
              """
                      {
                        "find": {
                          "sort": {
                             "address.street": 1
                          }
                        }
                      }
                      """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, allowManyIndexingCollection)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("SORT_PATH_UNINDEXED"))
          .body("errors[0].exceptionClass", is("SortException"))
          .body("errors[0].message", contains("sort path 'address.street' is not indexed"));
    }

    @Test
    public void fieldNameWithDot() {
      // allow "pricing.price&.usd", so one document is returned
      givenHeadersAndJson(
              """
                      {
                        "find": {
                          "filter": {
                            "pricing.price&.usd": 1
                          }
                        }
                      }
                      """)
          .post(CollectionResource.BASE_PATH, keyspaceName, allowManyIndexingCollection)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(1));

      // allow "pricing.price&&jpy", but the path is not escaped, so no document is returned
      givenHeadersAndJson(
              """
              {
                  "find": {
                  "filter": {
                      "pricing.price&jpy": 1
                  }
                  }
              }
              """)
          .post(CollectionResource.BASE_PATH, keyspaceName, allowManyIndexingCollection)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("FILTER_INVALID_EXPRESSION"))
          .body("errors[0].exceptionClass", is("FilterException"))
          .body(
              "errors[0].message",
              containsString(
                  "Unsupported filter clause: filter expression path ('pricing.price&jpy') is not valid: The ampersand"));

      // allow "metadata.app&.kubernetes&.io/name", so one document is returned
      givenHeadersAndJson(
              """
                        {
                            "find": {
                            "filter": {
                                "metadata.app&.kubernetes&.io/name": "test"
                            }
                            }
                        }
                        """)
          .post(CollectionResource.BASE_PATH, keyspaceName, allowManyIndexingCollection)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(1));

      // did not allow "pricing.price&.aud", even though the path is escaped, no document is
      // returned
      givenHeadersAndJson(
              """
                      {
                        "find": {
                          "filter": {
                            "pricing.price&&&.aud": 1
                          }
                        }
                      }
                      """)
          .post(CollectionResource.BASE_PATH, keyspaceName, allowManyIndexingCollection)
          .then()
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("FILTER_PATH_UNINDEXED"))
          .body("errors[0].exceptionClass", is("FilterException"))
          .body(
              "errors[0].message",
              containsString(
                  "Collection path 'pricing.price&&&.aud' is not indexed: cannot filter on that path"));
    }
  }
}
