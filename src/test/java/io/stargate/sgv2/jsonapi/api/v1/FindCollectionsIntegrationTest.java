package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsDDLSuccess;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsError;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.TestMethodOrder;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class FindCollectionsIntegrationTest extends AbstractKeyspaceIntegrationTestBase {

  @Nested
  @Order(1)
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class FindCollections {

    @Test
    @Order(1)
    /**
     * The keyspace that exists when database is created, and check if there is no collection in
     * this default keyspace.
     */
    public void checkNamespaceHasNoCollections() {
      givenHeadersPostJsonThenOkNoErrors(
              """
              {
                "findCollections": {
                }
              }
              """)
          .body("$", responseIsDDLSuccess())
          .body("status.collections", hasSize(0));
    }

    @Test
    @Order(2)
    public void happyPath() {
      // create first
      givenHeadersPostJsonThenOkNoErrors(
              """
                {
                  "createCollection": {
                    "name": "collection1"
                  }
                }
                """)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      // then find
      givenHeadersPostJsonThenOkNoErrors(
              """
                  {
                    "findCollections": { }
                  }
                  """)
          .body("$", responseIsDDLSuccess())
          .body("status.collections", hasSize(greaterThanOrEqualTo(1)))
          .body("status.collections", hasItem("collection1"));
    }

    @Test
    @Order(3)
    public void happyPathWithExplain() {
      // To create Collection with Lexical, it must be available for the database
      Assumptions.assumeTrue(isLexicalAvailableForDB());

      givenHeadersPostJsonThenOkNoErrors(
              """
              {
                "createCollection": {
                  "name": "collection2",
                  "options": {
                    "vector": {
                      "dimension": 5,
                      "metric": "cosine"
                    },
                    "indexing": {
                      "deny" : ["comment"]
                    },
                    "lexical": {
                      "enabled": true,
                      "analyzer": "standard"
                    },
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
              """)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      String expected1 =
          """
                  {
                    "name": "collection1",
                    "options":{
                      "lexical": {
                        "enabled": true,
                        "analyzer": "standard"
                      },
                      "rerank": {
                          "enabled": true,
                          "service": {
                              "provider": "nvidia",
                              "modelName": "nvidia/llama-3.2-nv-rerankqa-1b-v2"
                          }
                      }
                    }
                  }
                  """;
      String expected2 =
          """
              {
                  "name": "collection2",
                  "options": {
                    "vector": {
                      "dimension": 5,
                      "metric": "cosine",
                      "sourceModel": "other"
                    },
                    "indexing": {
                      "deny" : ["comment"]
                    },
                    "lexical": {
                      "enabled": true,
                      "analyzer": "standard"
                    },
                    "rerank": {
                        "enabled": true,
                        "service": {
                            "provider": "nvidia",
                            "modelName": "nvidia/llama-3.2-nv-rerankqa-1b-v2"
                        }
                    }
                  }
                }
              """;

      givenHeadersPostJsonThenOkNoErrors(
              """
              {
                "findCollections": {
                  "options": {
                    "explain" : true
                  }
                }
              }
              """)
          .body("$", responseIsDDLSuccess())
          .body("status.collections", hasSize(2))
          .body(
              "status.collections",
              containsInAnyOrder(jsonEquals(expected1), jsonEquals(expected2)));
    }

    @Test
    @Order(4)
    public void happyPathWithMixedCase() {
      givenHeadersPostJsonThenOkNoErrors(
              """
                                {
                                  "createCollection": {
                                    "name": "TableName"
                                  }
                                }
                                """)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      // then find
      givenHeadersPostJsonThenOkNoErrors(
              """
                                {
                                  "findCollections": { }
                                }
                                """)
          .body("$", responseIsDDLSuccess())
          .body("status.collections", hasSize(greaterThanOrEqualTo(1)))
          .body("status.collections", hasItem("TableName"));
    }

    @Test
    @Order(5)
    public void emptyNamespace() {
      // create namespace first
      String namespace = "nam" + RandomStringUtils.insecure().nextNumeric(16);
      givenHeadersAndJson(
                  """
          {
            "createNamespace": {
              "name": "%s"
            }
          }
          """
                  .formatted(namespace))
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      // then find
      givenHeadersAndJson(
              """
          {
            "findCollections": {
            }
          }
          """)
          .when()
          .post(KeyspaceResource.BASE_PATH, namespace)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.collections", hasSize(0));

      // cleanup
      givenHeadersAndJson(
                  """
          {
            "dropNamespace": {
              "name": "%s"
            }
          }
          """
                  .formatted(namespace))
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));
    }

    @Test
    @Order(6)
    public void notExistingNamespace() {
      givenHeadersAndJson(
              """
              {
                "findCollections": {
                }
              }
              """)
          .when()
          .post(KeyspaceResource.BASE_PATH, "should_not_be_there")
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("UNKNOWN_KEYSPACE"))
          .body(
              "errors[0].message",
              containsString(
                  "The command tried to use a Keyspace that does not exist in the Database"));
    }

    @Test
    @Order(7)
    public void happyPathIndexingWithExplain() {
      // To create Collection with Lexical, it must be available for the database
      Assumptions.assumeTrue(isLexicalAvailableForDB());

      givenHeadersPostJsonThenOkNoErrors(
              """
                  {
                    "createCollection": {
                      "name": "collection4",
                      "options": {
                        "defaultId" : {
                          "type" : "objectId"
                        },
                        "indexing": {
                          "deny" : ["comment"]
                        },
                        "lexical": {
                          "enabled": false
                        },
                        "rerank": {
                            "enabled": false
                        }
                      }
                    }
                  }
                  """)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      String expected1 =
          """
      {"name":"TableName",
       "options":{
        "lexical": {
          "enabled": true,
          "analyzer": "standard"
        },
        "rerank": {
            "enabled": true,
            "service": {
                "provider": "nvidia",
                "modelName": "nvidia/llama-3.2-nv-rerankqa-1b-v2"
            }
        }
      }}
      """;
      String expected2 =
          """
                  {
                    "name": "collection1",
                    "options": {
                      "lexical": {
                        "enabled": true,
                        "analyzer": "standard"
                      },
                        "rerank": {
                            "enabled": true,
                            "service": {
                                "provider": "nvidia",
                                "modelName": "nvidia/llama-3.2-nv-rerankqa-1b-v2"
                            }
                        }
                    }
                  }
              """;
      String expected3 =
          """
      {"name":"collection2",
        "options": {"vector": {"dimension":5, "metric":"cosine", "sourceModel": "other"},
        "indexing":{"deny":["comment"]},
        "lexical": {
            "enabled": true,
            "analyzer": "standard"
          },
            "rerank": {
                "enabled": true,
                "service": {
                    "provider": "nvidia",
                    "modelName": "nvidia/llama-3.2-nv-rerankqa-1b-v2"
                }
            }
      }}
      """;
      String expected4 =
          """
              {"name":"collection4",
               "options":{
                  "defaultId" : {"type" : "objectId"},
                  "indexing":{"deny":["comment"]},
                  "lexical": {
                    "enabled": false
                  },
                  "rerank": {
                      "enabled": false
                  }
                }
              }
              """;

      givenHeadersPostJsonThenOkNoErrors(
              """
                  {
                    "findCollections": {
                      "options": {
                        "explain" : true
                      }
                    }
                  }
                  """)
          .body("$", responseIsDDLSuccess())
          .body("status.collections", hasSize(4))
          .body(
              "status.collections",
              containsInAnyOrder(
                  jsonEquals(expected1),
                  jsonEquals(expected2),
                  jsonEquals(expected3),
                  jsonEquals(expected4)));
    }
  }

  @Nested
  @Order(2)
  class Metrics {
    @Test
    public void checkMetrics() {
      FindCollectionsIntegrationTest.super.checkMetrics("FindCollectionsCommand");
    }
  }
}
