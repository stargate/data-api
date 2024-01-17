package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.*;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class IndexingConfigIntegrationTest extends AbstractCollectionIntegrationTestBase {

  private static final String denyOneIndexingCollection = "deny_one_indexing_collection";

  private static final String denyManyIndexingCollection = "deny_many_indexing_collection";

  private static final String denyAllIndexingCollection = "deny_all_indexing_collection";

  private static final String allowOneIndexingCollection = "allow_one_indexing_collection";

  private static final String allowManyIndexingCollection = "allow_many_indexing_collection";

  @Nested
  @Order(1)
  class CreateCollectionAndData {

    @Test
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
              ]
            }
                      """;
      String denyOneList = """
              "deny" : ["address.city"]
              """;
      String denyManyList = """
              "deny" : ["name", "address", "contact.email"]
              """;
      String denyAllList = """
              "deny" : ["*"]
              """;
      String allowOneList = """
              "allow" : ["name"]
              """;
      String allowManyList = """
              "allow" : ["name", "address.city"]
              """;

      createCollection(denyOneIndexingCollection, denyOneList);
      createCollection(denyManyIndexingCollection, denyManyList);
      createCollection(denyAllIndexingCollection, denyAllList);
      createCollection(allowOneIndexingCollection, allowOneList);
      createCollection(allowManyIndexingCollection, allowManyList);

      insertDoc(denyOneIndexingCollection, insertData);
      insertDoc(denyManyIndexingCollection, insertData);
      insertDoc(denyAllIndexingCollection, insertData);
      insertDoc(allowOneIndexingCollection, insertData);
      insertDoc(allowManyIndexingCollection, insertData);
    }
  }

  @Nested
  @Order(2)
  class IndexingConfigTest {

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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(filterData)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, denyOneIndexingCollection)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
          .body("errors[0].message", endsWith("The filter path ('address.city') is not indexed"))
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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(filterData1)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, denyOneIndexingCollection)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(filterData2)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, denyOneIndexingCollection)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(filterData3)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, denyOneIndexingCollection)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
          .body("errors[0].message", endsWith("The filter path ('address.city') is not indexed"))
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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(filterData)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, denyOneIndexingCollection)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
          .body("errors[0].message", endsWith("The filter path ('address.city') is not indexed"))
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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(filterData)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, denyAllIndexingCollection)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
          .body("errors[0].message", endsWith("The filter path ('address.city') is not indexed"))
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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(filterId1)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, denyAllIndexingCollection)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(filterId2)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, denyAllIndexingCollection)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(filterId3)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, denyAllIndexingCollection)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
          .body(
              "errors[0].message",
              endsWith(
                  "The filter path ('_id') is not indexed, you can only use $eq or $in as the operator"))
          .body("errors[0].errorCode", is("ID_NOT_INDEXED"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(filterData)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, allowOneIndexingCollection)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(filterData1)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, allowOneIndexingCollection)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
          .body("errors[0].message", endsWith("The filter path ('address') is not indexed"))
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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(filterData2)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, allowOneIndexingCollection)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
          .body(
              "errors[0].message",
              endsWith(
                  "The filter path ('_id') is not indexed, you can only use $eq or $in as the operator"))
          .body("errors[0].errorCode", is("ID_NOT_INDEXED"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
      String filterData3 =
          """
                      {
                        "find": {
                          "filter": {"_id": "1"}
                        }
                      }
                        """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(filterData3)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, allowOneIndexingCollection)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(filterData)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, allowManyIndexingCollection)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(filterData1)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, allowManyIndexingCollection)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
          .body("errors[0].message", endsWith("The filter path ('address.street') is not indexed"))
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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(filterData2)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, allowManyIndexingCollection)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
          .body("errors[0].message", endsWith("The filter path ('address') is not indexed"))
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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(filterData1)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, denyOneIndexingCollection)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
          .body("errors[0].message", endsWith("The filter path ('address.city') is not indexed"))
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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(filterData2)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, denyOneIndexingCollection)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(filterData3)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, denyOneIndexingCollection)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
          .body("errors[0].message", endsWith("The filter path ('address.city') is not indexed"))
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
              .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
              .contentType(ContentType.JSON)
              .body(filterData4)
              .when()
              .post(CollectionResource.BASE_PATH, namespaceName, denyManyIndexingCollection)
              .then()
              .statusCode(200)
              .body("status", is(nullValue()))
              .body("data", is(nullValue()))
              .body("errors[0].message", endsWith("The filter path ('contact.email') is not indexed"))
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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(filterData1)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, denyOneIndexingCollection)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
          .body("errors[0].message", endsWith("The filter path ('address.city') is not indexed"))
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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(sortData)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, denyManyIndexingCollection)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents", hasSize(1));
    }

    @Test
    public void sortFieldNotInAllowMany() {
      // explicitly allow "name" "address.city", implicitly deny "_id" "address.street" "$vector"
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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(sortData)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, allowManyIndexingCollection)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
          .body("errors[0].message", endsWith("The sort path ('$vector') is not indexed"))
          .body("errors[0].errorCode", is("UNINDEXED_SORT_PATH"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }
  }
}
