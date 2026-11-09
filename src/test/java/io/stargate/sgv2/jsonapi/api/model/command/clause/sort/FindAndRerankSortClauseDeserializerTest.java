package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

import static io.stargate.sgv2.jsonapi.metrics.CommandFeature.*;
import static io.stargate.sgv2.jsonapi.util.Base64Util.encodeAsMimeBase64;
import static io.stargate.sgv2.jsonapi.util.CqlVectorUtil.floatsToBytes;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.FindAndRerankSort.HybridObject;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.FindAndRerankSort.HybridText;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.FindAndRerankSortDefinition.SingleLegDefinition;
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

/** Tests for the {@link FindAndRerankSortClauseDeserializer}. */
public class FindAndRerankSortClauseDeserializerTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(FindAndRerankSortClauseDeserializerTest.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private FindAndRerankSortClauseDeserializer deserializer;

  @Mock private JsonParser jsonParser;

  @Mock private DeserializationContext deserializationContext;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    deserializer = new FindAndRerankSortClauseDeserializer();
  }

  @Test
  public void testEqualsAndHash() {
    var value1 =
        new HybridObject(
            "vectorize sort",
            "lexical sort",
            new float[] {1.1f, 2.2f, 3.3f},
            CommandFeatures.EMPTY);
    var sameValue =
        new HybridObject(
            "vectorize sort",
            "lexical sort",
            new float[] {1.1f, 2.2f, 3.3f},
            CommandFeatures.EMPTY);

    var diffVectorize =
        new HybridObject(
            "vectorize sort 2",
            "lexical sort",
            new float[] {1.1f, 2.2f, 3.3f},
            CommandFeatures.EMPTY);
    var diffLexical =
        new HybridObject(
            "vectorize sort",
            "lexical sort 2",
            new float[] {1.1f, 2.2f, 3.3f},
            CommandFeatures.EMPTY);
    var diffVector =
        new HybridObject(
            "vectorize sort",
            "lexical sort",
            new float[] {1.1f, 2.2f, 3.3f, 4.4f},
            CommandFeatures.EMPTY);
    var diffForm = new HybridText("vectorize sort", CommandFeatures.EMPTY);

    var singleLeg =
        new SingleLegDefinition(
            sortJson(
                """
                {"$lexical": "cows"}
                """),
            CommandFeatures.EMPTY);
    var sameSingleLeg =
        new SingleLegDefinition(
            sortJson(
                """
                {"$lexical": "cows"}
                """),
            CommandFeatures.EMPTY);
    var diffSingleLeg =
        new SingleLegDefinition(
            sortJson(
                """
                {"$lexical": "sheep"}
                """),
            CommandFeatures.EMPTY);

    assertThat(value1).as("Object equals self").isEqualTo(value1);
    assertThat(value1).as("same values, vector compared by value").isEqualTo(sameValue);
    assertThat(value1).as("different form").isNotEqualTo(diffForm);
    assertThat(value1).as("different vectorize sort").isNotEqualTo(diffVectorize);
    assertThat(value1).as("different lexical sort").isNotEqualTo(diffLexical);
    assertThat(value1).as("different vector").isNotEqualTo(diffVector);
    assertThat(singleLeg).as("same single leg sort").isEqualTo(sameSingleLeg);
    assertThat(singleLeg).as("different single leg sort").isNotEqualTo(diffSingleLeg);

    assertThat(value1.hashCode()).as("hash code equals self").isEqualTo(value1.hashCode());
    assertThat(value1.hashCode())
        .as("hash code same values, vector compared by value")
        .isEqualTo(sameValue.hashCode());
    assertThat(value1.hashCode())
        .as("hash code different vectorize sort")
        .isNotEqualTo(diffVectorize.hashCode());
    assertThat(value1.hashCode())
        .as("hash code different lexical sort")
        .isNotEqualTo(diffLexical.hashCode());
    assertThat(value1.hashCode())
        .as("hash code different vector")
        .isNotEqualTo(diffVector.hashCode());
    assertThat(singleLeg.hashCode())
        .as("hash code same single leg sort")
        .isEqualTo(sameSingleLeg.hashCode());
    assertThat(singleLeg.hashCode())
        .as("hash code different single leg sort")
        .isNotEqualTo(diffSingleLeg.hashCode());
  }

  /** The parser reads a single leg sort as an object, so other JSON is a bug in the caller. */
  @Test
  public void singleLegDefinitionRejectsNonObject() {
    var ex =
        assertThrowsExactly(
            IllegalArgumentException.class,
            () -> new SingleLegDefinition(TextNode.valueOf("cows"), CommandFeatures.EMPTY));
    assertThat(ex).hasMessageContaining("sort must be an object or MissingNode");
  }

  @ParameterizedTest
  @MethodSource("validSortsTestCases")
  public void validSorts(String jsonString, FindAndRerankSortDefinition expected) throws Exception {

    LOGGER.debug("validSorts() - jsonString: {}", jsonString);
    ObjectMapper mapper = new ObjectMapper();
    when(deserializationContext.readTree(jsonParser)).thenReturn(mapper.readTree(jsonString));

    FindAndRerankSortDefinition actual =
        deserializer.deserialize(jsonParser, deserializationContext);
    LOGGER.debug("validSorts() - actual: {}", PrettyPrintable.pprint(actual));

    assertEquals(expected, actual);
  }

  private static Stream<Arguments> validSortsTestCases() {

    float[] emptyVector = new float[] {};
    String emptyVectorBase64 = encodeAsMimeBase64(floatsToBytes(emptyVector));
    float[] vector = new float[] {1.1f, 2.2f, 3.3f};
    String vectorBase64 = encodeAsMimeBase64(floatsToBytes(vector));

    return Stream.of(
        Arguments.of(
            """
            {}
            """,
            FindAndRerankSortDefinition.NO_ARG_SORT),
        Arguments.of(
            """
            null
            """,
            FindAndRerankSortDefinition.NO_ARG_SORT),
        // ----
        // $hybrid only
        Arguments.of(
            """
            { "$hybrid" : "same for hybrid and lexical" }
            """,
            new HybridText("same for hybrid and lexical", CommandFeatures.of(HYBRID))),
        Arguments.of(
            """
            { "$hybrid" : "  padded text  " }
            """,
            new HybridText("padded text", CommandFeatures.of(HYBRID))),
        Arguments.of(
            """
            { "$hybrid" : "" }
            """,
            new HybridText(null, CommandFeatures.of(HYBRID))),
        Arguments.of(
            """
            { "$hybrid" : "   " }
            """,
            new HybridText(null, CommandFeatures.of(HYBRID))),
        // ----
        // maximum fields, resolver works out the valid combinations
        Arguments.of(
            """
            { "$hybrid" : { "$vectorize" : "vectorize sort", "$lexical" : "lexical sort", "$vector" : [1.1, 2.2, 3.3]} }
            """,
            new HybridObject(
                "vectorize sort",
                "lexical sort",
                new float[] {1.1f, 2.2f, 3.3f},
                CommandFeatures.of(HYBRID, LEXICAL, VECTOR, VECTORIZE))),
        Arguments.of(
            """
            { "$hybrid" : { "$vectorize" : "vectorize sort", "$lexical" : "lexical sort"} }
            """,
            new HybridObject(
                "vectorize sort",
                "lexical sort",
                null,
                CommandFeatures.of(HYBRID, LEXICAL, VECTORIZE))),
        // ----
        // $lexical variations
        Arguments.of(
            """
            { "$hybrid" : { "$vectorize" : "vectorize sort", "$lexical" : null} }
            """,
            new HybridObject(
                "vectorize sort", null, null, CommandFeatures.of(HYBRID, VECTORIZE, LEXICAL))),
        Arguments.of(
            """
            { "$hybrid" : { "$vectorize" : "vectorize sort", "$lexical" : ""} }
            """,
            new HybridObject(
                "vectorize sort", null, null, CommandFeatures.of(HYBRID, VECTORIZE, LEXICAL))),
        Arguments.of(
            """
            { "$hybrid" : { "$vectorize" : "vectorize sort"} }
            """,
            new HybridObject("vectorize sort", null, null, CommandFeatures.of(HYBRID, VECTORIZE))),
        // ----
        // $vectorize variations
        Arguments.of(
            """
            { "$hybrid" : { "$vectorize" : "vectorize sort", "$lexical" : "lexical sort"} }
            """,
            new HybridObject(
                "vectorize sort",
                "lexical sort",
                null,
                CommandFeatures.of(HYBRID, LEXICAL, VECTORIZE))),
        Arguments.of(
            """
            { "$hybrid" : { "$vectorize" : null, "$lexical" : "lexical sort"} }
            """,
            new HybridObject(
                null, "lexical sort", null, CommandFeatures.of(HYBRID, LEXICAL, VECTORIZE))),
        Arguments.of(
            """
            { "$hybrid" : { "$vectorize" : "", "$lexical" : "lexical sort"} }
            """,
            new HybridObject(
                null, "lexical sort", null, CommandFeatures.of(HYBRID, LEXICAL, VECTORIZE))),
        Arguments.of(
            """
            { "$hybrid" : {"$lexical" : "lexical sort"} }
            """,
            new HybridObject(null, "lexical sort", null, CommandFeatures.of(HYBRID, LEXICAL))),
        // ----
        // $vector variations
        Arguments.of(
            """
            { "$hybrid" : { "$vectorize" : "vectorize", "$lexical" : "lexical", "$vector" : null} }
            """,
            new HybridObject(
                "vectorize",
                "lexical",
                null,
                CommandFeatures.of(HYBRID, LEXICAL, VECTORIZE, VECTOR))),
        Arguments.of(
            """
            { "$hybrid" : { "$vectorize" : "vectorize", "$lexical" : "lexical", "$vector" : [0.1, 0.2, 0.3]} }
            """,
            new HybridObject(
                "vectorize",
                "lexical",
                new float[] {0.1f, 0.2f, 0.3f},
                CommandFeatures.of(HYBRID, LEXICAL, VECTORIZE, VECTOR))),
        Arguments.of(
                """
            { "$hybrid" : { "$vectorize" : "vectorize", "$lexical" : "lexical", "$vector" : {"$binary": "%s"}} }
            """
                .formatted(emptyVectorBase64),
            new HybridObject(
                "vectorize",
                "lexical",
                emptyVector,
                CommandFeatures.of(HYBRID, LEXICAL, VECTORIZE, VECTOR))),
        Arguments.of(
                """
            { "$hybrid" : { "$vectorize" : "vectorize", "$lexical" : "lexical", "$vector" : {"$binary": "%s"}} }
            """
                .formatted(vectorBase64),
            new HybridObject(
                "vectorize",
                "lexical",
                vector,
                CommandFeatures.of(HYBRID, LEXICAL, VECTORIZE, VECTOR))));
  }

  /**
   * A sort without $hybrid is read by field name only and kept as sent, {@link
   * FindAndRerankSortParser} checks the values.
   */
  @ParameterizedTest
  @MethodSource("singleLegSortsTestCases")
  public void singleLegSorts(String jsonString, CommandFeatures expectedFeatures) throws Exception {

    LOGGER.debug("singleLegSorts() - jsonString: {}", jsonString);
    when(deserializationContext.readTree(jsonParser)).thenReturn(MAPPER.readTree(jsonString));

    FindAndRerankSortDefinition actual =
        deserializer.deserialize(jsonParser, deserializationContext);
    LOGGER.debug("singleLegSorts() - actual: {}", PrettyPrintable.pprint(actual));

    // the expected sort is a separate parse of the same JSON
    assertEquals(new SingleLegDefinition(sortJson(jsonString), expectedFeatures), actual);
  }

  private static Stream<Arguments> singleLegSortsTestCases() {

    String vectorBase64 = encodeAsMimeBase64(floatsToBytes(new float[] {1.1f, 2.2f, 3.3f}));

    return Stream.of(
        Arguments.of(
            """
            { "$vectorize" : "vectorize sort" }
            """,
            CommandFeatures.of(VECTORIZE)),
        Arguments.of(
            """
            { "$vector" : [1.1, 2.2, 3.3] }
            """,
            CommandFeatures.of(VECTOR)),
        Arguments.of(
                """
            { "$vector" : {"$binary": "%s"} }
            """
                .formatted(vectorBase64),
            CommandFeatures.of(VECTOR)),
        Arguments.of(
            """
            { "$lexical" : "lexical sort" }
            """,
            CommandFeatures.of(LEXICAL)),
        Arguments.of(
            """
            { "age" : -1, "name" : 1 }
            """,
            CommandFeatures.EMPTY),
        // ----
        // not checked here, FindAndRerankSortParser checks the values
        Arguments.of(
            """
            { "$lexical" : null }
            """,
            CommandFeatures.of(LEXICAL)),
        Arguments.of(
            """
            { "$vector" : [1.1, 2.2, 3.3], "age" : 1 }
            """,
            CommandFeatures.of(VECTOR)),
        Arguments.of(
            """
            { "age" : 2 }
            """,
            CommandFeatures.EMPTY));
  }

  @ParameterizedTest
  @MethodSource("invalidSortsTestCases")
  public void invalidSorts(String jsonString, String message) throws Exception {

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

  private static Stream<Arguments> invalidSortsTestCases() {
    return Stream.of(
        // ----
        // unexpected fields
        Arguments.of(
            """
            1
            """,
            "sort clause must be an object"),
        Arguments.of(
            """
            { "$hybrid" : "text", "fake" : 1}
            """,
            "Expected fields: $hybrid. Unexpected fields: fake"),
        Arguments.of(
            """
            { "$hybrid" : {"fake" : 1} }
            """,
            "Expected fields: $lexical, $vectorize, $vector. Unexpected fields: fake"),
        // ----
        // $hybrid only
        Arguments.of(
            """
            { "$hybrid" : 1}
            """,
            "Field $hybrid may only be of types TextNode, ObjectNode, but got: IntNode"),
        // ----
        // Combinations
        Arguments.of(
            """
            { "$hybrid" : { "$vectorize" : 1, "$lexical" : "", "$vector" : [1.1, 2.2, 3.3]} }
            """,
            "Field $vectorize may only be of types NullNode, TextNode, but got: IntNode"),
        Arguments.of(
            """
            { "$hybrid" : { "$vectorize" : "", "$lexical" : 1, "$vector" : [1.1, 2.2, 3.3]} }
            """,
            "Field $lexical may only be of types NullNode, TextNode, but got: IntNode"),
        Arguments.of(
            """
            { "$hybrid" : { "$vector" : "", "$lexical" : "", "$vector" : 1} }
            """,
            "Field $vector may only be of types NullNode, ArrayNode, ObjectNode, but got: IntNode"),
        Arguments.of(
            """
            { "$hybrid" : { "$vector" : "", "$lexical" : "", "$vector" : {"$date": "2023-01-01"}} }
            """,
            "`sort` clause (binary vector) contained fields with the wrong type. Field $vector may only be of types NullNode, ArrayNode, ObjectNode, but got: ObjectNode"));
  }

  /** The sort JSON as the deserializer gets it. */
  private static JsonNode sortJson(String json) {
    try {
      return MAPPER.readTree(json);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("test JSON does not parse: " + json, e);
    }
  }
}
