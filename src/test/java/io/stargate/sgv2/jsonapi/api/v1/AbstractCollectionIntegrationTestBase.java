package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

import io.restassured.http.ContentType;
import io.restassured.response.ValidatableResponse;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeAll;

/**
 * Abstract class for all int tests that needs a collection to execute tests in. This class
 * automatically creates a collection before all tests. Namespace handling is done by the super
 * class.
 *
 * <p>Note that this test uses a small workaround in {@link #getTestPort()} to avoid issue that
 * Quarkus is not setting-up the rest assured target port in the @BeforeAll and @AfterAll methods
 * (see https://github.com/quarkusio/quarkus/issues/7690).
 */
public abstract class AbstractCollectionIntegrationTestBase
    extends AbstractNamespaceIntegrationTestBase {

  // collection name automatically created in this test
  protected final String collectionName = "col" + RandomStringUtils.randomAlphanumeric(16);

  @BeforeAll
  public final void createSimpleCollection() {
    createSimpleCollection(this.collectionName);
  }

  protected void createSimpleCollection(String collectionToCreate) {
    given()
        .port(getTestPort())
        .headers(getHeaders())
        .contentType(ContentType.JSON)
        .body(
                """
              {
                "createCollection": {
                  "name": "%s"
                }
              }
              """
                .formatted(collectionToCreate))
        .when()
        .post(NamespaceResource.BASE_PATH, namespaceName)
        .then()
        .statusCode(200);
  }

  protected void createComplexCollection(String collectionSetting) {
    given()
        .port(getTestPort())
        .headers(getHeaders())
        .contentType(ContentType.JSON)
        .body(
                """
                      {
                        "createCollection": %s
                      }
                      """
                .formatted(collectionSetting))
        .when()
        .post(NamespaceResource.BASE_PATH, namespaceName)
        .then()
        .statusCode(200);
  }

  /** Utility to delete all documents from the test collection. */
  protected void deleteAllDocuments() {
    String json =
        """
        {
          "deleteMany": {
          }
        }
        """;

    while (true) {
      Boolean moreData =
          given()
              .headers(getHeaders())
              .contentType(ContentType.JSON)
              .body(json)
              .when()
              .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
              .then()
              .statusCode(200)
              .body("errors", is(nullValue()))
              .extract()
              .path("status.moreData");

      if (!Boolean.TRUE.equals(moreData)) {
        break;
      }
    }
  }

  /** Utility to insert a doc to the test collection. */
  protected void insertDoc(String docJson) {
    insertDoc(collectionName, docJson);
  }

  protected void insertDoc(String collection, String docJson) {
    String doc =
            """
            {
              "insertOne": {
                "document": %s
              }
            }
            """
            .formatted(docJson);

    given()
        .headers(getHeaders())
        .contentType(ContentType.JSON)
        .body(doc)
        .when()
        .post(CollectionResource.BASE_PATH, namespaceName, collection)
        .then()
        // Sanity check: let's look for non-empty inserted id
        .body("status.insertedIds[0]", not(emptyString()))
        .statusCode(200);
  }

  /** Utility to insert many docs to the test collection. */
  protected void insertManyDocs(String docsJson, int docsAmount) {
    String doc =
            """
                {
                  "insertMany": {
                    "documents": %s
                  }
                }
                """
            .formatted(docsJson);

    given()
        .headers(getHeaders())
        .contentType(ContentType.JSON)
        .body(doc)
        .when()
        .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
        .then()
        .body("status.insertedIds", hasSize(docsAmount))
        .statusCode(200);
  }

  /** Utility method for reducing boilerplate code for sending JSON commands */
  protected ValidatableResponse givenHeadersPostJsonThen(String json) {
    return given()
        .headers(getHeaders())
        .contentType(ContentType.JSON)
        .body(json)
        .when()
        .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
        .then();
  }

  /** Utility method for reducing boilerplate code for sending JSON commands */
  protected ValidatableResponse givenHeadersPostJsonThenOk(String json) {
    return givenHeadersPostJsonThen(json).statusCode(200);
  }

  /** Utility method for reducing boilerplate code for sending JSON commands */
  protected ValidatableResponse givenHeadersPostJsonThenOkNoErrors(String json) {
    return givenHeadersPostJsonThenOk(json).body("errors", is(nullValue()));
  }
}
