package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsFindSuccess;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsWriteSuccess;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.*;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class PaginationIntegrationTest extends AbstractCollectionIntegrationTestBase {

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  @Order(1)
  class NormalFunction {
    private static final int defaultPageSize = 20;
    private static final int documentAmount = 50;
    private static final int documentLimit = 5;

    @Test
    @Order(1)
    public void setUp() {
      for (int i = 0; i < documentAmount; i++) {
        insert(
                """
                              {
                                "insertOne": {
                                  "document": {
                                    "username": "testUser %s"
                                  }
                                }
                              }
                            """
                .formatted(i));
      }
    }

    private void insert(String json) {
      givenHeadersPostJsonThenOkNoErrors(json).body("$", responseIsWriteSuccess());
    }

    @Test
    @Order(2)
    public void threePagesCheck() {
      String nextPageState =
          givenHeadersPostJsonThenOkNoErrors(
                  """
                            {
                              "find": {
                              }
                            }
                            """)
              .body("$", responseIsFindSuccess())
              .body("data.documents", hasSize(defaultPageSize))
              .extract()
              .path("data.nextPageState");

      nextPageState =
          givenHeadersPostJsonThenOkNoErrors(
                      """
                    {
                      "find": {
                                "options":{
                                          "pageState" : "%s"
                                      }
                      }
                    }
                    """
                      .formatted(nextPageState))
              .body("$", responseIsFindSuccess())
              .body("data.documents", hasSize(defaultPageSize))
              .extract()
              .path("data.nextPageState");

      // should be fine with the empty sort clause
      givenHeadersPostJsonThenOkNoErrors(
                  """
                  {
                      "find": {
                          "sort": {},
                          "options": {
                              "pageState": "%s"
                          }
                      }
                  }
                """
                  .formatted(nextPageState))
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(documentAmount - 2 * defaultPageSize))
          .body("data.nextPageState", nullValue());
    }

    @Test
    @Order(3)
    public void pageLimitCheck() {
      givenHeadersPostJsonThenOkNoErrors(
                  """
                            {
                              "find": {
                                        "options": {
                                                  "limit": %s
                                              }
                              }
                            }
                            """
                  .formatted(documentLimit))
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(documentLimit))
          .body("data.nextPageState", nullValue());
    }
  }
}
