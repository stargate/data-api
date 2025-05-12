package io.stargate.sgv2.jsonapi.api.model.command.impl;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.metrics.CommandFeature;
import io.stargate.sgv2.jsonapi.metrics.CommandFeatures;
import io.stargate.sgv2.jsonapi.util.recordable.PrettyPrintable;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests for the {@link FindAndRerankCommand.HybridLimitsDeserializer}. */
public class HybridLimitsDeserializerTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(HybridLimitsDeserializerTest.class);

  private FindAndRerankCommand.HybridLimitsDeserializer deserializer;

  @Mock private JsonParser jsonParser;

  @Mock private DeserializationContext deserializationContext;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    deserializer = new FindAndRerankCommand.HybridLimitsDeserializer();
  }

  @Test
  public void testEqualsAndHash() {
    var value1 = new FindAndRerankCommand.HybridLimits(10, 10, CommandFeatures.EMPTY);

    var diffVector = new FindAndRerankCommand.HybridLimits(20, 10, CommandFeatures.EMPTY);
    var diffLexical = new FindAndRerankCommand.HybridLimits(10, 20, CommandFeatures.EMPTY);

    assertThat(value1).as("Object equals self").isEqualTo(value1);
    assertThat(value1).as("different vector limit").isNotEqualTo(diffVector);
    assertThat(value1).as("different lexical limit").isNotEqualTo(diffLexical);

    assertThat(value1.hashCode()).as("hash code equals self").isEqualTo(value1.hashCode());
    assertThat(value1.hashCode())
        .as("hash code different vector limit")
        .isNotEqualTo(diffVector.hashCode());
    assertThat(value1.hashCode())
        .as("hash code different lexical limit")
        .isNotEqualTo(diffLexical.hashCode());
  }

  @ParameterizedTest
  @MethodSource("validLimitsTestCases")
  public void validLimits(String jsonString, FindAndRerankCommand.HybridLimits expected)
      throws Exception {

    LOGGER.debug("validLimits() - jsonString: {}", jsonString);
    ObjectMapper mapper = new ObjectMapper();
    when(deserializationContext.readTree(jsonParser)).thenReturn(mapper.readTree(jsonString));

    var actual = deserializer.deserialize(jsonParser, deserializationContext);
    LOGGER.debug("validLimits() - actual: {}", PrettyPrintable.pprint(actual));

    assertEquals(expected, actual);
  }

  private static Stream<Arguments> validLimitsTestCases() {
    return Stream.of(
        // ----
        // same
        Arguments.of(
            """
            99
            """,
            new FindAndRerankCommand.HybridLimits(
                99, 99, CommandFeatures.of(CommandFeature.HYBRID_LIMITS_NUMBER))),
        Arguments.of(
            """
            0
            """,
            new FindAndRerankCommand.HybridLimits(
                0, 0, CommandFeatures.of(CommandFeature.HYBRID_LIMITS_NUMBER))),
        // ----
        // all must tbe provided for the object form
        Arguments.of(
            """
            { "$vector" : 99, "$lexical" : 99}
            """,
            new FindAndRerankCommand.HybridLimits(
                99,
                99,
                CommandFeatures.of(
                    CommandFeature.HYBRID_LIMITS_VECTOR, CommandFeature.HYBRID_LIMITS_LEXICAL))),
        Arguments.of(
            """
            { "$vector" : 9, "$lexical" : 99}
            """,
            new FindAndRerankCommand.HybridLimits(
                9,
                99,
                CommandFeatures.of(
                    CommandFeature.HYBRID_LIMITS_VECTOR, CommandFeature.HYBRID_LIMITS_LEXICAL))));
  }

  @ParameterizedTest
  @MethodSource("invalidLimitsTestCases")
  public void invalidLimits(String jsonString, String message) throws Exception {

    LOGGER.debug("invalidSorts() - jsonString: {}", jsonString);
    ObjectMapper mapper = new ObjectMapper();
    when(deserializationContext.readTree(jsonParser)).thenReturn(mapper.readTree(jsonString));

    var error =
        assertThrows(
            JsonMappingException.class,
            () -> deserializer.deserialize(jsonParser, deserializationContext));
    LOGGER.debug("validSorts() - error.getMessage(): {}", error.getMessage());

    assertThat(error).isInstanceOf(JsonMappingException.class).hasMessageContaining(message);
  }

  private static Stream<Arguments> invalidLimitsTestCases() {
    return Stream.of(
        // ----
        Arguments.of(
            """
            true
            """,
            "hybridLimits must be an integer or an object"),
        // ----
        // out of range
        Arguments.of(
            """
            -1
            """,
            "hybridLimits must be zero or greater, got -1 for $vector"),
        Arguments.of(
            """
            { "$vector" : -1, "$lexical" : 99}
            """,
            "hybridLimits must be zero or greater, got -1 for $vector"),
        Arguments.of(
            """
            { "$vector" : 99, "$lexical" : -1}
            """,
            "hybridLimits must be zero or greater, got -1 for $lexical"),
        Arguments.of(
            """
            { "$vector" : -1, "$lexical" : -1}
            """,
            "hybridLimits must be zero or greater, got -1 for $vector"),
        // ----
        // unexpected
        Arguments.of(
            """
            { "fake" : 99}
            """,
            "Expected fields: $lexical, $vector. Unexpected fields: fake"),
        // ----
        // missing
        Arguments.of(
            """
            {"$lexical" : -1}
            """,
            "Expected fields: $lexical, $vector. Missing fields: $vector"),
        Arguments.of(
            """
            {"$vector" : -1}
            """,
            "Expected fields: $lexical, $vector. Missing fields: $lexical"));
  }
}
