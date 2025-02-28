package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsError;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsFindSuccess;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.*;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
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
      String filterData =
          """
              {
                "find": {
                  "filter": {"address.city": "monkey town"}
                }
              }
                """;
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(filterData)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, denyOneIndexingCollection)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].message", endsWith("filter path 'address.city' is not indexed"))
          .body("errors[0].errorCode", is("UNINDEXED_FILTER_PATH"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }

    @Test
    public void filterVectorFieldInDenyAll() {
      // explicitly deny "address.city", implicitly allow "_id", "name", "address.street"
      String filterData =
          """
                  {
                    "find": {
                      "filter": {"$vector": {"$exists": true}}
                    }
                  }
                    """;
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(filterData)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, denyAllIndexingCollection)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].message", endsWith("filter path '$vector' is not indexed"))
          .body("errors[0].errorCode", is("UNINDEXED_FILTER_PATH"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }

    @Test
    public void filterFieldNotInDenyOne() {
      // explicitly deny "address.city", implicitly allow "_id", "name", "address.street"
      String filterData1 =
          """
              {
                "find": {
                  "filter": {"name": "aaron"}
                }
              }
                """;
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(filterData1)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, denyOneIndexingCollection)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(1));
      // deny "address.city", only this as a string, not "address" as an object
      String filterData2 =
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
                    """;
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(filterData2)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, denyOneIndexingCollection)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(1));
      String filterData3 =
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
                        """;
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(filterData3)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, denyOneIndexingCollection)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].message", endsWith("filter path 'address.city' is not indexed"))
          .body("errors[0].errorCode", is("UNINDEXED_FILTER_PATH"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }

    @Test
    public void filterFieldInDenyMany() {
      // explicitly deny "name", "address", implicitly allow "_id"
      // deny "address", "address.city" should also be included
      String filterData =
          """
                  {
                    "find": {
                      "filter": {"address.city": "monkey town"}
                    }
                  }
                    """;
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(filterData)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, denyManyIndexingCollection)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].message", endsWith("filter path 'address.city' is not indexed"))
          .body("errors[0].errorCode", is("UNINDEXED_FILTER_PATH"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }

    @Test
    public void filterFieldInDenyAll() {
      // deny all use "*"
      String filterData =
          """
                  {
                    "find": {
                      "filter": {"address.city": "monkey town"}
                    }
                  }
                    """;
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(filterData)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, denyAllIndexingCollection)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].message", endsWith("filter path 'address.city' is not indexed"))
          .body("errors[0].errorCode", is("UNINDEXED_FILTER_PATH"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }

    @Test
    public void filterIdInDenyAllWithEqAndIn() {
      // deny all use "*"
      String filterId1 =
          """
                  {
                    "find": {
                      "filter": {"_id": "1"}
                    }
                  }
                    """;
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(filterId1)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, denyAllIndexingCollection)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(1));

      String filterId2 =
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
                      """;
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(filterId2)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, denyAllIndexingCollection)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(1));
    }

    @Test
    public void filterIdInDenyAllWithoutEqAndIn() {
      // deny all use "*"
      String filterId3 =
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
                    """;
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(filterId3)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, denyAllIndexingCollection)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("ID_NOT_INDEXED"))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body(
              "errors[0].message",
              is("_id is not indexed: you can only use $eq or $in as the operator"));
    }

    @Test
    public void filterFieldInAllowOne() {
      // explicitly allow "name", implicitly deny "_id" "address"
      String filterData =
          """
                  {
                    "find": {
                      "filter": {"name": "aaron"}
                    }
                  }
                    """;
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(filterData)
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
      String filterData1 =
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
                        """;
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(filterData1)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, allowOneIndexingCollection)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].message", endsWith("filter path 'address' is not indexed"))
          .body("errors[0].errorCode", is("UNINDEXED_FILTER_PATH"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
      String filterData2 =
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
                        """;
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(filterData2)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, allowOneIndexingCollection)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("ID_NOT_INDEXED"))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body(
              "errors[0].message",
              is("_id is not indexed: you can only use $eq or $in as the operator"));
      String filterData3 =
          """
                      {
                        "find": {
                          "filter": {"_id": "1"}
                        }
                      }
                        """;
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(filterData3)
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
      String filterData =
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
                            """;
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(filterData)
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
      String filterData1 =
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
                            """;
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(filterData1)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, allowManyIndexingCollection)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].message", endsWith("filter path 'address.street' is not indexed"))
          .body("errors[0].errorCode", is("UNINDEXED_FILTER_PATH"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
      // allow "address.city", only this as a string, not "address" as an object
      String filterData2 =
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
                    """;

      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(filterData2)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, allowManyIndexingCollection)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].message", endsWith("filter path 'address' is not indexed"))
          .body("errors[0].errorCode", is("UNINDEXED_FILTER_PATH"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }

    @Test
    public void incrementalPathInArray() {
      // explicitly deny "address.city", implicitly allow "_id", "name", "address.street" "address"
      // String and array in array - no incremental path, the path is "address" - should be allowed
      // but no data return
      String filterData1 =
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
                  """;
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(filterData1)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, denyOneIndexingCollection)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].message", endsWith("filter path 'address.city' is not indexed"))
          .body("errors[0].errorCode", is("UNINDEXED_FILTER_PATH"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
      // explicitly deny "address.city", implicitly allow "_id", "name", "address.street" "address"
      String filterData2 =
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
                      """;
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(filterData2)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, denyOneIndexingCollection)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(1));
      // explicitly deny "address.city", implicitly allow "_id", "name", "address.street" "address"
      // Object (Hashmap) in array - incremental path is "address.city"
      String filterData3 =
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
                      """;
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(filterData3)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, denyOneIndexingCollection)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].message", endsWith("filter path 'address.city' is not indexed"))
          .body("errors[0].errorCode", is("UNINDEXED_FILTER_PATH"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
      // explicitly deny "name", "address" "contact.email"
      String filterData4 =
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
                          """;
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(filterData4)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, denyManyIndexingCollection)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].message", endsWith("filter path 'contact.email' is not indexed"))
          .body("errors[0].errorCode", is("UNINDEXED_FILTER_PATH"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }

    @Test
    public void incrementalPathInMap() {
      // explicitly deny "address.city", implicitly allow "_id", "name", "address.street" "address"
      // map in map
      String filterData1 =
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
                      """;
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(filterData1)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, denyOneIndexingCollection)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].message", endsWith("filter path 'address.city' is not indexed"))
          .body("errors[0].errorCode", is("UNINDEXED_FILTER_PATH"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }

    @Test
    public void sortFieldInAllowMany() {
      // explicitly deny "name", "address", implicitly allow "_id", "$vector"
      String sortData =
          """
                  {
                    "find": {
                      "sort": {
                         "$vector" : [0.15, 0.1, 0.1, 0.35, 0.55]
                      }
                    }
                  }
                          """;
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(sortData)
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
      String sortData =
          """
                      {
                        "find": {
                          "sort": {
                             "address.street": 1
                          }
                        }
                      }
                              """;
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(sortData)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, allowManyIndexingCollection)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].message", endsWith("sort path 'address.street' is not indexed"))
          .body("errors[0].errorCode", is("UNINDEXED_SORT_PATH"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }

    @Test
    public void fieldNameWithDot() {
      // allow "pricing.price&.usd", so one document is returned
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
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
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
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
          .body(
              "errors[0].message",
              containsString("filter clause path ('pricing.price&jpy') is not a valid path."))
          .body("errors[0].errorCode", is("INVALID_FILTER_EXPRESSION"))
          .body("errors[0].exceptionClass", is("JsonApiException"));

      // allow "metadata.app&.kubernetes&.io/name", so one document is returned
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
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
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
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
          .body(
              "errors[0].message",
              containsString(
                  "Unindexed filter path: filter path 'pricing.price&&&.aud' is not indexed"))
          .body("errors[0].errorCode", is("UNINDEXED_FILTER_PATH"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }
  }
}
