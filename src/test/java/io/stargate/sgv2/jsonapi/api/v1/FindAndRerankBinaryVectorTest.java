package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsError;
import static io.stargate.sgv2.jsonapi.util.Base64Util.encodeAsMimeBase64;
import static io.stargate.sgv2.jsonapi.util.CqlVectorUtil.floatsToBytes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindAndRerankCommand;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.exception.RequestException;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests request parsing and HTTP error mapping without a database or reranking provider. */
@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class FindAndRerankBinaryVectorTest {

  @Inject ObjectMapper objectMapper;

  @ParameterizedTest
  @ValueSource(
      strings = {"\"\"", "\" \"", "\"not base64!\"", "\"AAA=\"", "123", "true", "null", "[]", "{}"})
  void invalidBinaryVectorReturnsRequestError(String binaryValue) {
    // Parsing must reject the request before looking up the collection or calling a provider.
    given()
        .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "test-token")
        .contentType(ContentType.JSON)
        .body(commandWithBinaryVector(binaryValue))
        .when()
        .post(CollectionResource.BASE_PATH, "test_keyspace", "test_collection")
        .then()
        .statusCode(200)
        .body("$", responseIsError())
        .body("errors.size()", is(1))
        .body("errors[0].family", is("REQUEST"))
        .body("errors[0].errorCode", is(RequestException.Code.REQUEST_STRUCTURE_MISMATCH.name()))
        .body("errors[0].message", containsString("sort.$hybrid.$vector.$binary"));
  }

  @Test
  void validBinaryVectorIsDecoded() throws Exception {
    float[] vector = new float[] {1.1f, 2.2f, 3.3f};
    String binaryValue = objectMapper.writeValueAsString(encodeAsMimeBase64(floatsToBytes(vector)));

    FindAndRerankCommand command =
        objectMapper.readValue(commandWithBinaryVector(binaryValue), FindAndRerankCommand.class);

    assertThat(command.sortClause().vectorSort()).containsExactly(vector);
  }

  private static String commandWithBinaryVector(String binaryValue) {
    return
        """
        {"findAndRerank": {
          "sort": {"$hybrid": {"$vector": {"$binary": %s}}},
          "options": {"rerankQuery": "I like cheese!", "rerankOn": "content", "limit": 10}
        }}
        """
        .formatted(binaryValue);
  }
}
