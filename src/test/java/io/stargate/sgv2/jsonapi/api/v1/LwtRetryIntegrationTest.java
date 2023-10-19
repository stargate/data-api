package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.RepeatedTest;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
public class LwtRetryIntegrationTest extends AbstractCollectionIntegrationTestBase {

  @RepeatedTest(10)
  public void mixedOperations() throws Exception {
    String document =
        """
        {
          "_id": "doc1",
          "count": 0
        }
        """;
    insertDoc(document);

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
              "$inc": {"count": 1}
            }
          }
        }
        """;

    CountDownLatch latch = new CountDownLatch(2);

    // we know that delete must be executed eventually
    // but for update we might see delete before read, before update or after
    new Thread(
            () -> {
              given()
                  .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
                  .contentType(ContentType.JSON)
                  .body(update)
                  .when()
                  .post(CollectionResource.BASE_PATH, collectionName)
                  .then()
                  .statusCode(200)
                  .body("status.matchedCount", anyOf(is(0), is(1)))
                  .body("status.modifiedCount", anyOf(is(0), is(1)))
                  .body("data", is(nullValue()))
                  .body("errors", is(nullValue()));

              latch.countDown();
            })
        .start();

    new Thread(
            () -> {
              given()
                  .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
                  .contentType(ContentType.JSON)
                  .body(delete)
                  .when()
                  .post(CollectionResource.BASE_PATH, collectionName)
                  .then()
                  .statusCode(200)
                  .body("status.deletedCount", is(1))
                  .body("data", is(nullValue()))
                  .body("errors", is(nullValue()));

              latch.countDown();
            })
        .start();

    latch.await();

    // ensure there's nothing left
    String json = """
        {
          "find": {
          }
        }
        """;
    given()
        .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
        .contentType(ContentType.JSON)
        .body(json)
        .when()
        .post(CollectionResource.BASE_PATH, collectionName)
        .then()
        .statusCode(200)
        .body("data.documents", is(empty()));
  }

  @AfterEach
  public void cleanUpData() {
    deleteAllDocuments();
  }
}
