package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsFindSuccess;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;

import io.quarkus.test.common.WithTestResource;
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
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class FindOneWithSortIntegrationTest extends AbstractCollectionIntegrationTestBase {
  @Nested
  @Order(1)
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class FindOneSortWithDottedPaths {
    private final String DOC1 =
        """
        {
          "_id": "dottedSort1",
          "type": "sorted",
          "app.kubernetes.io/name": "dottedZ"
        }
        """;

    private final String DOC2 =
        """
        {
          "_id": "dottedSort2",
          "type": "sorted",
          "app.kubernetes.io/name": "dottedY"
        }
        """;
    private final String DOC3 =
        """
        {
          "_id": "dottedSort3",
          "type": "sorted",
          "app.kubernetes.io/name": "dottedX"
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
    public void sortByDottedField() {
      // Reversed order compared to _id
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
          .body("data.document", jsonEquals(DOC3));

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
  }
}
