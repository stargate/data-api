package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsError;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.FindAndRerankSortClauseDeserializer;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.exception.SortException;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests that an error {@link FindAndRerankSortClauseDeserializer} throws for a single leg sort
 * reaches HTTP users with its error code and message.
 *
 * <p>The requests fail when the command is deserialized, before any database access, so the test
 * does not need a database.
 */
@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class FindAndRerankSortRequestTest {

  @ParameterizedTest(name = "{0}")
  @MethodSource("invalidSingleLegSorts")
  void returnsSortErrorForInvalidSingleLegSort(
      String sort, SortException.Code code, String message) {
    given()
        .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "test-token")
        .contentType(ContentType.JSON)
        .body(
                """
            {"findAndRerank": {"sort": %s}}
            """
                .formatted(sort))
        .when()
        .post(CollectionResource.BASE_PATH, "keyspace", "collection")
        .then()
        .statusCode(200)
        .body("$", responseIsError())
        .body("errors", hasSize(1))
        .body("errors[0].errorCode", is(code.name()))
        .body("errors[0].message", containsString(message));
  }

  static Stream<Arguments> invalidSingleLegSorts() {
    return Stream.of(
        Arguments.of(
            """
            {"$lexical": "cows", "age": 1}
            """,
            SortException.Code.SORT_CLAUSE_INVALID,
            "when sorting by field '$lexical' no other sort expressions allowed"),
        Arguments.of(
            """
            {"$fake": 1}
            """,
            SortException.Code.SORT_CLAUSE_PATH_INVALID,
            "path cannot start with '$'"),
        Arguments.of(
            """
            {"age": 2}
            """,
            SortException.Code.SORT_CLAUSE_VALUE_INVALID,
            "sort ordering value can only be `1` for ascending or `-1` for descending (not `2`)"));
  }
}
