package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
public class DeleteManyIntegrationTest extends CollectionResourceBaseIntegrationTest {
  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class DeleteMany {

    private void insert(int countOfDocument) {
      for (int i = 1; i <= countOfDocument; i++) {
        String json =
            """
            {
              "insertOne": {
                "document": {
                  "_id": "doc%s",
                  "username": "user%s",
                  "status": "active"
                }
              }
            }
            """;

        given()
            .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
            .contentType(ContentType.JSON)
            .body(json.formatted(i, i))
            .when()
            .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
            .then()
            .statusCode(200)
            .body("errors", is(nullValue()));
      }
    }

    @Test
    public void deleteManyById() {
      insert(2);
      String json =
          """
          {
            "deleteMany": {
              "filter" : {"_id" : "doc1"}
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("status.deletedCount", is(1))
          .body("status.moreData", is(nullValue()))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));

      // ensure find does not find the document
      json =
          """
          {
            "findOne": {
              "filter" : {"_id" : "doc1"}
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.docs", jsonEquals("[]"))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));

      // but can find does the non-deleted document
      json =
          """
          {
            "findOne": {
              "filter" : {"_id" : "doc2"}
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.docs[0]._id", is("doc2"))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void deleteManyByColumn() {
      insert(5);
      String json =
          """
          {
            "deleteMany": {
              "filter" : {"status": "active"}
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("status.deletedCount", is(5))
          .body("status.moreData", is(nullValue()))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));

      // ensure find does not find the documents
      json =
          """
          {
            "find": {
              "filter" : {"status": "active"}
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.docs", jsonEquals("[]"))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void deleteManyNoFilter() {
      insert(20);
      String json =
          """
          {
            "deleteMany": {
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("status.deletedCount", is(20))
          .body("status.moreData", is(nullValue()))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));

      // ensure find does not find the documents
      json = """
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
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.docs", jsonEquals("[]"))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void deleteManyNoFilterMoreDataFlag() {
      insert(25);
      String json =
          """
          {
            "deleteMany": {
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("status.deletedCount", is(20))
          .body("status.moreData", is(true))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));

      // ensure only 20 are really deleted
      json = """
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
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.docs", hasSize(5))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @AfterEach
    public void cleanUpData() {
      deleteAllDocuments();
    }
  }
}
