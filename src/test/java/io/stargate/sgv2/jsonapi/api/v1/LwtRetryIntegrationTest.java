package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsFindSuccess;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsStatusOnly;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.RepeatedTest;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
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
                  .headers(getHeaders())
                  .contentType(ContentType.JSON)
                  .body(update)
                  .when()
                  .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
                  .then()
                  .statusCode(200)
                  .body("$", responseIsStatusOnly())
                  .body("status.matchedCount", anyOf(is(0), is(1)))
                  .body("status.modifiedCount", anyOf(is(0), is(1)));

              latch.countDown();
            })
        .start();

    new Thread(
            () -> {
              given()
                  .headers(getHeaders())
                  .contentType(ContentType.JSON)
                  .body(delete)
                  .when()
                  .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
                  .then()
                  .statusCode(200)
                  .body("$", responseIsStatusOnly())
                  .body("status.deletedCount", is(1));

              latch.countDown();
            })
        .start();

    latch.await();

    // ensure there's nothing left
    String json =
        """
        {
          "find": {
          }
        }
        """;
    given()
        .headers(getHeaders())
        .contentType(ContentType.JSON)
        .body(json)
        .when()
        .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
        .then()
        .statusCode(200)
        .body("$", responseIsFindSuccess())
        .body("data.documents", is(empty()));
  }

  @AfterEach
  public void cleanUpData() {
    deleteAllDocuments();
  }
}
