package io.stargate.sgv2.jsonapi.api.model.command.impl;

import static io.stargate.sgv2.jsonapi.util.asserts.DataAPIAsserts.assertThatSchemaException;
import static org.assertj.core.api.Assertions.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Unit tests for the {@link CreateCollectionCommand.Options.IndexingDesc} */
public class IndexingDescTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(IndexingDescTest.class);

  private final TestConstants TEST_CONSTANTS = new TestConstants();

  public CreateCollectionCommand.Options.IndexingDesc deserialize(String testName, String json) {
    LOGGER.info("deserialize() - testName: {}, json: {}", testName, json);

    try {
      return TEST_CONSTANTS.OBJECT_MAPPER.readValue(
          json, CreateCollectionCommand.Options.IndexingDesc.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public void assertSchemaException(
      String testName, String json, ErrorCode<?> errorCode, String... message) {

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

    assertSchemaException(
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

    assertSchemaException(
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

    assertSchemaException(
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

    assertSchemaException(
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

    assertSchemaException(
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

    assertSchemaException(
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

    assertSchemaException(
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

    assertSchemaException(
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

    assertSchemaException(
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

    assertSchemaException(
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

    var indexingDesc = deserialize("successAllowStar()", json);
    indexingDesc.validate();
    assertThat(indexingDesc.allow()).containsExactly("*");
    assertThat(indexingDesc.deny()).isNull();
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

    var indexingDesc = deserialize("successDenyStar()", json);
    indexingDesc.validate();
    assertThat(indexingDesc.deny()).containsExactly("*");
    assertThat(indexingDesc.denyAll()).as("delayAll true when deny '*'").isTrue();
    assertThat(indexingDesc.allow()).isNull();
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

    var indexingDesc = deserialize("successAllowVector()", json);
    indexingDesc.validate();
    assertThat(indexingDesc.allow()).containsExactly("$vector");
    assertThat(indexingDesc.deny()).isNull();
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

    var indexingDesc = deserialize("successDenyVector()", json);
    indexingDesc.validate();
    assertThat(indexingDesc.deny()).containsExactly("$vector");
    assertThat(indexingDesc.allow()).isNull();
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

    var indexingDesc = deserialize("successAllowValidPaths()", json);
    indexingDesc.validate();
    assertThat(indexingDesc.allow()).containsExactly("name", "address.city", "tags");
    assertThat(indexingDesc.deny()).isNull();
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

    var indexingDesc = deserialize("successDenyValidPaths()", json);
    indexingDesc.validate();
    assertThat(indexingDesc.deny()).containsExactly("name", "address.city", "tags");
    assertThat(indexingDesc.allow()).isNull();
  }
}
