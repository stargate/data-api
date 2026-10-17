package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsError;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.exception.SortException;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class FindAndRerankRequestTest {

  @ParameterizedTest
  @ValueSource(
      strings = {
        "{ \"$vectorize\" : \"cheese\", \"$lexical\" : \"cows\" }",
        "{ \"$hybrid\" : \"cheese\", \"$vectorize\" : \"cheese\" }"
      })
  void failOnMultipleTopLevelSortFields(String sort) {
    // The request fails during deserialization before any database access.
    given()
        .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "test-token")
        .contentType(ContentType.JSON)
        .body("{\"findAndRerank\": {\"sort\": %s}}".formatted(sort))
        .when()
        .post(CollectionResource.BASE_PATH, "keyspace", "collection")
        .then()
        .statusCode(200)
        .body("$", responseIsError())
        .body("errors", hasSize(1))
        .body("errors[0].errorCode", is(SortException.Code.SORT_CLAUSE_INVALID.name()))
        .body(
            "errors[0].message",
            containsString(
                "sort clause must have exactly one of $hybrid, $vectorize, $vector, $lexical"));
  }
}
