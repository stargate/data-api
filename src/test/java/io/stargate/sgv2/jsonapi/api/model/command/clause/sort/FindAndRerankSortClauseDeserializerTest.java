package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

import static io.stargate.sgv2.jsonapi.metrics.CommandFeature.*;
import static io.stargate.sgv2.jsonapi.util.Base64Util.encodeAsMimeBase64;
import static io.stargate.sgv2.jsonapi.util.CqlVectorUtil.floatsToBytes;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.metrics.CommandFeatures;
import io.stargate.sgv2.jsonapi.util.recordable.PrettyPrintable;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
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
        new FindAndRerankSort(
            SortMode.HYBRID,
            false,
            "vectorize sort",
            "lexical sort",
            new float[] {1.1f, 2.2f, 3.3f},
            CommandFeatures.EMPTY);

    var diffVectorize =
        new FindAndRerankSort(
            SortMode.HYBRID,
            false,
            "vectorize sort 2",
            "lexical sort",
            new float[] {1.1f, 2.2f, 3.3f},
            CommandFeatures.EMPTY);
    var diffLexical =
        new FindAndRerankSort(
            SortMode.HYBRID,
            false,
            "vectorize sort",
            "lexical sort 2",
            new float[] {1.1f, 2.2f, 3.3f},
            CommandFeatures.EMPTY);
    var diffVector =
        new FindAndRerankSort(
            SortMode.HYBRID,
            false,
            "vectorize sort",
            "lexical sort",
            new float[] {1.1f, 2.2f, 3.3f, 4.4f},
            CommandFeatures.EMPTY);
    var diffExplicitLexical =
        new FindAndRerankSort(
            SortMode.HYBRID,
            true,
            "vectorize sort",
            "lexical sort",
            new float[] {1.1f, 2.2f, 3.3f},
            CommandFeatures.EMPTY);
    // a FILTER sort has no values, so compare it with a HYBRID sort that has no values
    var hybridNoValues =
        new FindAndRerankSort(SortMode.HYBRID, false, null, null, null, CommandFeatures.EMPTY);
    var filterNoValues =
        new FindAndRerankSort(SortMode.FILTER, false, null, null, null, CommandFeatures.EMPTY);

    assertThat(value1).as("Object equals self").isEqualTo(value1);
    assertThat(value1).as("different vectorize sort").isNotEqualTo(diffVectorize);
    assertThat(value1).as("different lexical sort").isNotEqualTo(diffLexical);
    assertThat(value1).as("different vector").isNotEqualTo(diffVector);
    assertThat(value1).as("different explicitLexical").isNotEqualTo(diffExplicitLexical);
    assertThat(hybridNoValues).as("different sortMode").isNotEqualTo(filterNoValues);

    assertThat(value1.hashCode()).as("hash code equals self").isEqualTo(value1.hashCode());
    assertThat(value1.hashCode())
        .as("hash code different vectorize sort")
        .isNotEqualTo(diffVectorize.hashCode());
    assertThat(value1.hashCode())
        .as("hash code different lexical sort")
        .isNotEqualTo(diffLexical.hashCode());
    assertThat(value1.hashCode())
        .as("hash code different vector")
        .isNotEqualTo(diffVector.hashCode());
    assertThat(value1.hashCode())
        .as("hash code different explicitLexical")
        .isNotEqualTo(diffExplicitLexical.hashCode());
    assertThat(hybridNoValues.hashCode())
        .as("hash code different sortMode")
        .isNotEqualTo(filterNoValues.hashCode());
  }

  @Test
  public void noArgSortIsNewForEachCall() {
    var first = FindAndRerankSort.noArgSort();
    var second = FindAndRerankSort.noArgSort();

    assertThat(first)
        .as("FILTER sort with no values and no features")
        .isEqualTo(
            new FindAndRerankSort(
                SortMode.FILTER, false, null, null, null, CommandFeatures.create()));
    assertThat(first).as("new sort each call").isNotSameAs(second);
    assertThat(first.commandFeatures())
        .as("new commandFeatures each call")
        .isNotSameAs(second.commandFeatures())
        .isNotSameAs(CommandFeatures.EMPTY);

    // a feature added for one request must not leak into another request
    first.commandFeatures().addFeature(HYBRID);
    assertThat(second.commandFeatures()).as("no leak").isEqualTo(CommandFeatures.create());
    assertThat(FindAndRerankSort.noArgSort().commandFeatures())
        .as("no leak into later sorts")
        .isEqualTo(CommandFeatures.create());
  }

  @Test
  public void failFilterSortWithValues() {
    assertFilterSortFails(
        () ->
            new FindAndRerankSort(
                SortMode.FILTER, true, null, null, null, CommandFeatures.create()),
        "explicitLexical must be false when sortMode is FILTER");
    assertFilterSortFails(
        () ->
            new FindAndRerankSort(
                SortMode.FILTER, false, "cheese", null, null, CommandFeatures.create()),
        "vectorizeSort must be null when sortMode is FILTER, vectorizeSort=cheese");
    assertFilterSortFails(
        () ->
            new FindAndRerankSort(
                SortMode.FILTER, false, null, "cows", null, CommandFeatures.create()),
        "lexicalSort must be null when sortMode is FILTER, lexicalSort=cows");
    assertFilterSortFails(
        () ->
            new FindAndRerankSort(
                SortMode.FILTER,
                false,
                null,
                null,
                new float[] {1.1f, 2.2f},
                CommandFeatures.create()),
        "vectorSort must be null when sortMode is FILTER, vectorSort.length=2");
  }

  @Test
  public void failMissingSortModeOrCommandFeatures() {
    var sortModeEx =
        assertThrows(
            NullPointerException.class,
            () -> new FindAndRerankSort(null, false, null, null, null, CommandFeatures.create()));
    assertThat(sortModeEx).hasMessage("sortMode must not be null");

    var featuresEx =
        assertThrows(
            NullPointerException.class,
            () -> new FindAndRerankSort(SortMode.HYBRID, false, "cheese", null, null, null));
    assertThat(featuresEx).hasMessage("commandFeatures must not be null");
  }

  private static void assertFilterSortFails(Executable newSort, String expectedMessage) {
    var ex = assertThrows(IllegalArgumentException.class, newSort);
    assertThat(ex).hasMessage(expectedMessage);
  }

  @ParameterizedTest
  @MethodSource("validSortsTestCases")
  public void validSorts(String jsonString, FindAndRerankSort expected) throws Exception {

    LOGGER.debug("validSorts() - jsonString: {}", jsonString);
    ObjectMapper mapper = new ObjectMapper();
    when(deserializationContext.readTree(jsonParser)).thenReturn(mapper.readTree(jsonString));

    FindAndRerankSort actual = deserializer.deserialize(jsonParser, deserializationContext);
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
            FindAndRerankSort.noArgSort()),
        Arguments.of(
            """
            null
            """,
            FindAndRerankSort.noArgSort()),
        // ----
        // $hybrid only
        Arguments.of(
            """
            { "$hybrid" : "same for hybrid and lexical" }
            """,
            new FindAndRerankSort(
                SortMode.HYBRID,
                false,
                "same for hybrid and lexical",
                "same for hybrid and lexical",
                null,
                CommandFeatures.of(HYBRID))),
        Arguments.of(
            """
            { "$hybrid" : "" }
            """,
            new FindAndRerankSort(
                SortMode.HYBRID, false, null, null, null, CommandFeatures.of(HYBRID))),
        // ----
        // maximum fields, resolver works out the valid combinations
        Arguments.of(
            """
            { "$hybrid" : { "$vectorize" : "vectorize sort", "$lexical" : "lexical sort", "$vector" : [1.1, 2.2, 3.3]} }
            """,
            new FindAndRerankSort(
                SortMode.HYBRID,
                true,
                "vectorize sort",
                "lexical sort",
                new float[] {1.1f, 2.2f, 3.3f},
                CommandFeatures.of(HYBRID, LEXICAL, VECTOR, VECTORIZE))),
        Arguments.of(
            """
            { "$hybrid" : { "$vectorize" : "vectorize sort", "$lexical" : "lexical sort"} }
            """,
            new FindAndRerankSort(
                SortMode.HYBRID,
                true,
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
            new FindAndRerankSort(
                SortMode.HYBRID,
                true,
                "vectorize sort",
                null,
                null,
                CommandFeatures.of(HYBRID, VECTORIZE, LEXICAL))),
        Arguments.of(
            """
            { "$hybrid" : { "$vectorize" : "vectorize sort", "$lexical" : ""} }
            """,
            new FindAndRerankSort(
                SortMode.HYBRID,
                true,
                "vectorize sort",
                null,
                null,
                CommandFeatures.of(HYBRID, VECTORIZE, LEXICAL))),
        Arguments.of(
            """
            { "$hybrid" : { "$vectorize" : "vectorize sort"} }
            """,
            new FindAndRerankSort(
                SortMode.HYBRID,
                false,
                "vectorize sort",
                null,
                null,
                CommandFeatures.of(HYBRID, VECTORIZE))),
        // ----
        // $vectorize variations
        Arguments.of(
            """
            { "$hybrid" : { "$vectorize" : "vectorize sort", "$lexical" : "lexical sort"} }
            """,
            new FindAndRerankSort(
                SortMode.HYBRID,
                true,
                "vectorize sort",
                "lexical sort",
                null,
                CommandFeatures.of(HYBRID, LEXICAL, VECTORIZE))),
        Arguments.of(
            """
            { "$hybrid" : { "$vectorize" : null, "$lexical" : "lexical sort"} }
            """,
            new FindAndRerankSort(
                SortMode.HYBRID,
                true,
                null,
                "lexical sort",
                null,
                CommandFeatures.of(HYBRID, LEXICAL, VECTORIZE))),
        Arguments.of(
            """
            { "$hybrid" : { "$vectorize" : "", "$lexical" : "lexical sort"} }
            """,
            new FindAndRerankSort(
                SortMode.HYBRID,
                true,
                null,
                "lexical sort",
                null,
                CommandFeatures.of(HYBRID, LEXICAL, VECTORIZE))),
        Arguments.of(
            """
            { "$hybrid" : {"$lexical" : "lexical sort"} }
            """,
            new FindAndRerankSort(
                SortMode.HYBRID,
                true,
                null,
                "lexical sort",
                null,
                CommandFeatures.of(HYBRID, LEXICAL))),
        // ----
        // $vector variations
        Arguments.of(
            """
            { "$hybrid" : { "$vectorize" : "vectorize", "$lexical" : "lexical", "$vector" : null} }
            """,
            new FindAndRerankSort(
                SortMode.HYBRID,
                true,
                "vectorize",
                "lexical",
                null,
                CommandFeatures.of(HYBRID, LEXICAL, VECTORIZE, VECTOR))),
        Arguments.of(
            """
            { "$hybrid" : { "$vectorize" : "vectorize", "$lexical" : "lexical", "$vector" : [0.1, 0.2, 0.3]} }
            """,
            new FindAndRerankSort(
                SortMode.HYBRID,
                true,
                "vectorize",
                "lexical",
                new float[] {0.1f, 0.2f, 0.3f},
                CommandFeatures.of(HYBRID, LEXICAL, VECTORIZE, VECTOR))),
        Arguments.of(
                """
            { "$hybrid" : { "$vectorize" : "vectorize", "$lexical" : "lexical", "$vector" : {"$binary": "%s"}} }
            """
                .formatted(emptyVectorBase64),
            new FindAndRerankSort(
                SortMode.HYBRID,
                true,
                "vectorize",
                "lexical",
                emptyVector,
                CommandFeatures.of(HYBRID, LEXICAL, VECTORIZE, VECTOR))),
        Arguments.of(
                """
            { "$hybrid" : { "$vectorize" : "vectorize", "$lexical" : "lexical", "$vector" : {"$binary": "%s"}} }
            """
                .formatted(vectorBase64),
            new FindAndRerankSort(
                SortMode.HYBRID,
                true,
                "vectorize",
                "lexical",
                vector,
                CommandFeatures.of(HYBRID, LEXICAL, VECTORIZE, VECTOR))));
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
            { "fake" : 1}
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
}
