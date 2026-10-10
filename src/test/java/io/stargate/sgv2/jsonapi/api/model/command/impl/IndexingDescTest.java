package io.stargate.sgv2.jsonapi.api.model.command.impl;

import static io.stargate.sgv2.jsonapi.util.asserts.DataAPIAsserts.assertThatSchemaException;
import static org.assertj.core.api.Assertions.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit tests for the {@link CreateCollectionCommand.Options.IndexingDesc}
 *
 * <p>We are testing that when created from JSON and validated, exceptions are thrown and functions
 * derived from the record state work. Not really testing that deserialisationw works.
 */
public class IndexingDescTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(IndexingDescTest.class);

  private final TestConstants TEST_CONSTANTS = new TestConstants();

  private CreateCollectionCommand.Options.IndexingDesc assertValidate(
      String testName, String json, CreateCollectionCommand.Options.IndexingDesc expected) {

    LOGGER.info("assertValidate() - testName: {}, json:{}", testName, json);

    var actualIndexingDesc = deserialize(testName, json);
    actualIndexingDesc.validate();

    assertThat(actualIndexingDesc).as(testName).isEqualTo(expected);
    return actualIndexingDesc;
  }

  /** Assert that validating the JSON for the IndexingDesc will result in a SchemaException */
  private void assertValidateFails(
      String testName, String json, ErrorCode<?> errorCode, String... message) {

    LOGGER.info("assertValidateFails() - testName: {}, json:{}", testName, json);

    var indexingDesc = deserialize(testName, json);
    var throwable = catchThrowable(indexingDesc::validate);

    assertThatSchemaException(throwable)
        .as(testName)
        .hasCode(errorCode)
        .hasMessageSnippets(message);
  }

  @Test
  public void failAllowAndDeny() {

    var json =
        """
        {
            "deny": [
                "comment"
            ],
            "allow": [
                "data"
            ]
        }""";

    assertValidateFails(
        "failAllowAndDeny()",
        json,
        SchemaException.Code.INVALID_INDEXING_DEFINITION,
        "'createCollection' indexing definition invalid: 'allow' and 'deny' cannot be used together");
  }

  @Test
  public void failNeitherAllowNorDeny() {

    var json =
        """
        {}""";

    assertValidateFails(
        "failNeitherAllowNorDeny()",
        json,
        SchemaException.Code.INVALID_INDEXING_DEFINITION,
        "'allow' or 'deny' should be provided");
  }

  @Test
  public void failAllowDuplicates() {

    var json =
        """
        {
            "allow": [
                "name",
                "name"
            ]
        }""";

    assertValidateFails(
        "failAllowDuplicates()",
        json,
        SchemaException.Code.INVALID_INDEXING_DEFINITION,
        "'allow' cannot contain duplicates");
  }

  @Test
  public void failDenyDuplicates() {

    var json =
        """
         {
             "deny": [
                 "name",
                 "name"
             ]
         }""";

    assertValidateFails(
        "failDenyDuplicates()",
        json,
        SchemaException.Code.INVALID_INDEXING_DEFINITION,
        "'deny' cannot contain duplicates");
  }

  @Test
  public void failAllowEmptyPath() {

    var json =
        """
        {
            "allow": [
                ""
            ]
        }""";

    assertValidateFails(
        "failAllowEmptyPath()",
        json,
        SchemaException.Code.INVALID_INDEXING_DEFINITION,
        "path must be represented as a non-empty string");
  }

  @Test
  public void failDenyEmptyPath() {

    var json =
        """
        {
            "deny": [
                ""
            ]
        }""";

    assertValidateFails(
        "failDenyEmptyPath()",
        json,
        SchemaException.Code.INVALID_INDEXING_DEFINITION,
        "path must be represented as a non-empty string");
  }

  @Test
  public void failAllowDollarPrefix() {

    var json =
        """
        {
            "allow": [
                "$score"
            ]
        }""";

    assertValidateFails(
        "failAllowDollarPrefix()",
        json,
        SchemaException.Code.INVALID_INDEXING_DEFINITION,
        "path ('$score') must not start with '$'");
  }

  @Test
  public void failDenyDollarPrefix() {

    var json =
        """
        {
            "deny": [
                "$score"
            ]
        }""";

    assertValidateFails(
        "failDenyDollarPrefix()",
        json,
        SchemaException.Code.INVALID_INDEXING_DEFINITION,
        "path ('$score') must not start with '$'");
  }

  @Test
  public void failAllowInvalidPath() {

    var json =
        """
        {
            "allow": [
                "bad&path"
            ]
        }""";

    assertValidateFails(
        "failAllowInvalidPath()",
        json,
        SchemaException.Code.INVALID_INDEXING_DEFINITION,
        "indexing path ('bad&path') is not a valid path");
  }

  @Test
  public void failDenyInvalidPath() {

    var json =
        """
        {
            "deny": [
                "bad&path"
            ]
        }""";

    assertValidateFails(
        "failDenyInvalidPath()",
        json,
        SchemaException.Code.INVALID_INDEXING_DEFINITION,
        "indexing path ('bad&path') is not a valid path");
  }

  @Test
  public void successAllowStar() {

    var json =
        """
        {
            "allow": [
                "*"
            ]
        }""";

    var expected = new CreateCollectionCommand.Options.IndexingDesc(List.of("*"), null);
    assertValidate("successAllowStar()", json, expected);
  }

  @Test
  public void successDenyStar() {

    var json =
        """
        {
            "deny": [
                "*"
            ]
        }""";

    var expected = new CreateCollectionCommand.Options.IndexingDesc(null, List.of("*"));
    var actual = assertValidate("successDenyStar()", json, expected);
    assertThat(actual.denyAll()).as("delayAll true when deny '*'").isTrue();
  }

  @Test
  public void successAllowVector() {

    var json =
        """
        {
            "allow": [
                "$vector"
            ]
        }""";

    var expected = new CreateCollectionCommand.Options.IndexingDesc(List.of("$vector"), null);
    assertValidate("successAllowVector()", json, expected);
  }

  @Test
  public void successDenyVector() {

    var json =
        """
        {
            "deny": [
                "$vector"
            ]
        }""";

    var expected = new CreateCollectionCommand.Options.IndexingDesc(null, List.of("$vector"));
    assertValidate("successDenyVector()", json, expected);
  }

  @Test
  public void successAllowValidPaths() {

    var json =
        """
        {
            "allow": [
                "name",
                "address.city",
                "tags"
            ]
        }""";

    var expected =
        new CreateCollectionCommand.Options.IndexingDesc(
            List.of("name", "address.city", "tags"), null);
    assertValidate("successAllowValidPaths()", json, expected);
  }

  @Test
  public void successDenyValidPaths() {

    var json =
        """
        {
            "deny": [
                "name",
                "address.city",
                "tags"
            ]
        }""";

    var expected =
        new CreateCollectionCommand.Options.IndexingDesc(
            null, List.of("name", "address.city", "tags"));
    assertValidate("successDenyValidPaths()", json, expected);
  }

  private CreateCollectionCommand.Options.IndexingDesc deserialize(String testName, String json) {
    LOGGER.info("deserialize() - testName: {}, json: {}", testName, json);

    try {
      return TEST_CONSTANTS.OBJECT_MAPPER.readValue(
          json, CreateCollectionCommand.Options.IndexingDesc.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
