package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import java.util.List;
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
  public final void createCollection() {
    String json =
        """
        {
          "createCollection": {
            "name": "%s"
          }
        }
        """
            .formatted(collectionName);

    given()
        .port(getTestPort())
        .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
        .contentType(ContentType.JSON)
        .body(json)
        .when()
        .post(NamespaceResource.BASE_PATH, namespaceName)
        .then()
        .statusCode(200);
  }

  /** Utility to delete all documents from the test collection. */
  protected void deleteAllDocuments() {
    String json = """
        {
          "deleteMany": {
          }
        }
        """;

    while (true) {
      Boolean moreData =
          given()
              .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
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
        .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
        .contentType(ContentType.JSON)
        .body(doc)
        .when()
        .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
        .then()
        // Sanity check: let's look for non-empty inserted id
        .body("status.insertedIds[0]", not(emptyString()))
        .statusCode(200);
  }

  public void checkMetrics(String commandName) {
    String metrics = given().when().get("/metrics").then().statusCode(200).extract().asString();
    List<String> countMetrics =
        metrics
            .lines()
            .filter(
                line ->
                    line.startsWith("command_processor_count")
                        && line.contains("command=\"" + commandName + "\""))
            .toList();
    assertThat(countMetrics.size()).isGreaterThan(0);

    List<String> totalMetrics =
        metrics
            .lines()
            .filter(
                line ->
                    line.startsWith("command_processor_total")
                        && line.contains("command=\"" + commandName + "\""))
            .toList();
    assertThat(totalMetrics.size()).isGreaterThan(0);
  }
}
