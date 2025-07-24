package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsError;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsFindSuccess;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.TestMethodOrder;

@QuarkusIntegrationTest
@QuarkusTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class FindOneWithSortIntegrationTest extends AbstractCollectionIntegrationTestBase {
  @Nested
  @Order(1)
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class OkWithDottedPaths {
    private final String DOC1 =
        """
        {
          "_id": "dottedSort1",
          "type": "sorted",
          "app.kubernetes.io/name": "dottedZ",
          "metadata": {
            "shape": "square"
          }
        }
        """;

    private final String DOC2 =
        """
        {
          "_id": "dottedSort2",
          "type": "sorted",
          "app.kubernetes.io/name": "dottedY",
          "metadata": {
            "shape": "triangle"
          }
        }
        """;
    private final String DOC3 =
        """
        {
          "_id": "dottedSort3",
          "type": "sorted",
          "app.kubernetes.io/name": "dottedX",
          "metadata": {
            "shape": "ellipse"
          }
        }
        """;

    @Test
    @Order(1)
    public void setUp() {
      insertDoc(DOC1);
      insertDoc(DOC2);
      insertDoc(DOC3);
    }

    // First: tests by regular doc id
    @Test
    public void sortById() {
      givenHeadersPostJsonThenOkNoErrors(
              """
                            {
                              "findOne": {
                                "filter" : {"type": "sorted"},
                                "sort" : {"_id": 1}
                              }
                            }
                            """)
          .body("$", responseIsFindSuccess())
          .body("data.document", jsonEquals(DOC1));
      givenHeadersPostJsonThenOkNoErrors(
              """
                            {
                              "findOne": {
                                "filter" : {"type": "sorted"},
                                "sort" : {"_id": -1}
                              }
                            }
                            """)
          .body("$", responseIsFindSuccess())
          .body("data.document", jsonEquals(DOC3));
    }

    @Test
    public void sortByDottedFieldWithEscape() {
      // Reversed order compared to _id
      givenHeadersPostJsonThenOkNoErrors(
              """
                      {
                        "findOne": {
                          "filter" : {"type": "sorted"},
                          "sort" : {"app&.kubernetes&.io/name": 1}
                        }
                      }
                      """)
          .body("$", responseIsFindSuccess())
          .body("data.document", jsonEquals(DOC3));

      givenHeadersPostJsonThenOkNoErrors(
              """
                      {
                        "findOne": {
                          "filter" : {"type": "sorted"},
                          "sort" : {"app&.kubernetes&.io/name": -1}
                        }
                      }
                      """)
          .body("$", responseIsFindSuccess())
          .body("data.document", jsonEquals(DOC1));
    }

    @Test
    public void sortByDottedFieldWithoutEscape() {
      // without correct escape, the order will be the first _id
      givenHeadersPostJsonThenOkNoErrors(
              """
                        {
                            "findOne": {
                            "filter" : {"type": "sorted"},
                            "sort" : {"app.kubernetes.io/name": 1}
                            }
                        }
                        """)
          .body("$", responseIsFindSuccess())
          .body("data.document", jsonEquals(DOC1));

      givenHeadersPostJsonThenOkNoErrors(
              """
                        {
                            "findOne": {
                            "filter" : {"type": "sorted"},
                            "sort" : {"app.kubernetes.io/name": -1}
                            }
                        }
                        """)
          .body("$", responseIsFindSuccess())
          .body("data.document", jsonEquals(DOC1));
    }

    @Test
    public void sortByNestedField() {
      // Ordering by nested shape yet different from other orders
      givenHeadersPostJsonThenOkNoErrors(
              """
                      {
                        "findOne": {
                          "filter" : {"type": "sorted"},
                          "sort" : {"metadata.shape": 1}
                        }
                      }
                      """)
          .body("$", responseIsFindSuccess())
          .body("data.document", jsonEquals(DOC3));

      givenHeadersPostJsonThenOkNoErrors(
              """
                      {
                        "findOne": {
                          "filter" : {"type": "sorted"},
                          "sort" : {"metadata.shape": -1}
                        }
                      }
                      """)
          .body("$", responseIsFindSuccess())
          .body("data.document", jsonEquals(DOC2));
    }

    @Test
    public void sortByNestedFieldWithEscape() {
      // cannot find the result, the order will be the first _id
      givenHeadersPostJsonThenOkNoErrors(
              """
                      {
                        "findOne": {
                          "filter" : {"type": "sorted"},
                          "sort" : {"metadata&.shape": 1}
                        }
                      }
                      """)
          .body("$", responseIsFindSuccess())
          .body("data.document", jsonEquals(DOC1));

      givenHeadersPostJsonThenOkNoErrors(
              """
                      {
                        "findOne": {
                          "filter" : {"type": "sorted"},
                          "sort" : {"metadata&.shape": -1}
                        }
                      }
                      """)
          .body("$", responseIsFindSuccess())
          .body("data.document", jsonEquals(DOC1));
    }
  }

  @Nested
  @Order(2)
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class FailDueToInvalidPaths {
    @Test
    void failOnEmptySortPath() {
      givenHeadersPostJsonThenOk(
              """
                  { "find": { "sort" : {"" : 1} } }
                  """)
          .body("$", responseIsError())
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("INVALID_SORT_CLAUSE_PATH"))
          .body("errors[0].message", endsWith("path must be represented as a non-empty string"));
    }

    @Test
    void failOnLeadingDollarInSortPath() {
      givenHeadersPostJsonThenOk(
              """
                  { "find": { "sort" : {"$gt" : 1} } }
                  """)
          .body("$", responseIsError())
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("INVALID_SORT_CLAUSE_PATH"))
          .body("errors[0].message", containsString("path ('$gt') cannot start with '$'"));
    }

    @Test
    void failOnInvalidEscapedSortPath() {
      givenHeadersPostJsonThenOk(
              """
                  { "find": { "sort" : {"a&b" : 1} } }
                  """)
          .body("$", responseIsError())
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("INVALID_SORT_CLAUSE_PATH"))
          .body(
              "errors[0].message",
              containsString(
                  "Invalid sort clause path: sort clause path ('a&b') is not a valid path."));
    }
  }
}
