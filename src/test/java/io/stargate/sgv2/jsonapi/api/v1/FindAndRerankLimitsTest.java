package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsError;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.exception.RequestException;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests limit validation and HTTP error mapping without a database or reranking provider. */
@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class FindAndRerankLimitsTest {

  @ParameterizedTest
  @MethodSource("invalidLimits")
  void invalidLimitReturnsRequestError(String options, String path) {
    given()
        .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "test-token")
        .contentType(ContentType.JSON)
        .body(
                """
            {"findAndRerank": {
              "sort": {"$hybrid": "abc"},
              "options": {
                "rerankQuery": "I like cheese!", "rerankOn": "content", %s
              }
            }}
            """
                .formatted(options))
        .when()
        .post(CollectionResource.BASE_PATH, "test_keyspace", "test_collection")
        .then()
        .statusCode(200)
        .body("$", responseIsError())
        .body("errors.size()", is(1))
        .body("errors[0].family", is("REQUEST"))
        .body("errors[0].errorCode", is(RequestException.Code.REQUEST_STRUCTURE_MISMATCH.name()))
        .body("errors[0].message", containsString(path))
        .body("errors[0].message", containsString("must be an integer"));
  }

  private static Stream<Arguments> invalidLimits() {
    return Stream.of(
            "0.5",
            "5.9",
            "50.9",
            "5.0",
            "5e0",
            "\"5\"",
            "3000000000",
            "-3000000000",
            "4294967306",
            "-4294967286",
            "18446744073709551626")
        .flatMap(
            value ->
                Stream.of(
                    Arguments.of(
                        "\"limit\": %s, \"hybridLimits\": 10".formatted(value), "options.limit"),
                    Arguments.of(
                        "\"limit\": 10, \"hybridLimits\": %s".formatted(value),
                        "options.hybridLimits"),
                    Arguments.of(
                        "\"hybridLimits\": {\"$vector\": %s, \"$lexical\": 10}".formatted(value),
                        "options.hybridLimits.$vector"),
                    Arguments.of(
                        "\"hybridLimits\": {\"$vector\": 10, \"$lexical\": %s}".formatted(value),
                        "options.hybridLimits.$lexical")));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, -1})
  void integerOuterLimitStillRequiresPositiveValue(int limit) {
    given()
        .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "test-token")
        .contentType(ContentType.JSON)
        .body("{\"findAndRerank\": {\"options\": {\"limit\": %d}}}".formatted(limit))
        .when()
        .post(CollectionResource.BASE_PATH, "test_keyspace", "test_collection")
        .then()
        .statusCode(200)
        .body("$", responseIsError())
        .body("errors[0].family", is("REQUEST"))
        .body("errors[0].errorCode", is(RequestException.Code.COMMAND_FIELD_VALUE_INVALID.name()))
        .body("errors[0].message", containsString("limit should be greater than `0`"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"5.0", "5e0"})
  void explainsRejectionOfWholeValuedFloatingPointNumbers(String value) {
    given()
        .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "test-token")
        .contentType(ContentType.JSON)
        .body("{\"findAndRerank\": {\"options\": {\"limit\": %s}}}".formatted(value))
        .when()
        .post(CollectionResource.BASE_PATH, "test_keyspace", "test_collection")
        .then()
        .statusCode(200)
        .body("$", responseIsError())
        .body("errors[0].family", is("REQUEST"))
        .body("errors[0].message", containsString("options.limit"))
        .body("errors[0].message", containsString("floating-point number"));
  }
}
