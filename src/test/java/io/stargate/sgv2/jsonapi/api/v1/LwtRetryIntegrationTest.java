package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.UUID;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
public class LwtRetryIntegrationTest extends CollectionResourceBaseIntegrationTest {
  @Test
  @Order(1)
  public void setUp() {
    String json =
        """
            {
              "insertOne": {
                "document": {
                  "_id": "doc1",
                  "username": "user1",
                  "active_user" : true
                }
              }
            }
            """;

    insert(json);
  }

  private void insert(String json) {
    given()
        .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
        .contentType(ContentType.JSON)
        .body(json)
        .when()
        .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
        .then()
        .statusCode(200);
  }
  /**
   * Made the invocation to 3, so all the transactions should be successful because retrylimit is 3
   */
  @Test
  @Order(2)
  @Execution(ExecutionMode.CONCURRENT)
  @RepeatedTest(3)
  public void retryLWT() {
    String delete =
        """
          {
              "deleteOne": {
                "filter": {
                  "_id": "doc1"
                }
              }
            }
      """;

    String update =
        """
          {
              "updateOne": {
                "filter": {
                  "_id": "doc1"
                },
                "update" : {
                    $set": {"counter": "%s"}
                },
                "options": {"upsert" : true}
              }
            }
        """;

    given()
        .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
        .contentType(ContentType.JSON)
        .body(update.formatted(UUID.randomUUID().toString()))
        .when()
        .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
        .then()
        .statusCode(200)
        .body("errors", is(nullValue()));
    given()
        .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
        .contentType(ContentType.JSON)
        .body(delete)
        .when()
        .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
        .then()
        .statusCode(200)
        .body("errors", is(nullValue()));
  }
}
