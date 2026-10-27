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
import io.stargate.sgv2.jsonapi.exception.DocumentException;
import io.stargate.sgv2.jsonapi.exception.SortException;
import io.stargate.sgv2.jsonapi.metrics.CommandFeatures;
import io.stargate.sgv2.jsonapi.util.recordable.PrettyPrintable;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
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
            LegMode.HYBRID,
            null,
            "vectorize sort",
            "lexical sort",
            new float[] {1.1f, 2.2f, 3.3f},
            CommandFeatures.EMPTY);

    var diffVectorize =
        new FindAndRerankSort(
            LegMode.HYBRID,
            null,
            "vectorize sort 2",
            "lexical sort",
            new float[] {1.1f, 2.2f, 3.3f},
            CommandFeatures.EMPTY);
    var diffLexical =
        new FindAndRerankSort(
            LegMode.HYBRID,
            null,
            "vectorize sort",
            "lexical sort 2",
            new float[] {1.1f, 2.2f, 3.3f},
            CommandFeatures.EMPTY);
    var diffVector =
        new FindAndRerankSort(
            LegMode.HYBRID,
            null,
            "vectorize sort",
            "lexical sort",
            new float[] {1.1f, 2.2f, 3.3f, 4.4f},
            CommandFeatures.EMPTY);
    // the $hybrid text cannot be set with the object values, so compare it with other text
    var hybridText =
        new FindAndRerankSort(LegMode.HYBRID, "cheese", null, null, null, CommandFeatures.EMPTY);
    var diffHybridText =
        new FindAndRerankSort(LegMode.HYBRID, "cows", null, null, null, CommandFeatures.EMPTY);
    // {"$hybrid": "cheese"} and {"$hybrid": {"$vectorize": "cheese", "$lexical": "cheese"}}
    var hybridObjectSameText =
        new FindAndRerankSort(
            LegMode.HYBRID, null, "cheese", "cheese", null, CommandFeatures.EMPTY);
    // a FILTER sort has no values, so compare it with a HYBRID sort that has no values
    var hybridNoValues =
        new FindAndRerankSort(LegMode.HYBRID, null, null, null, null, CommandFeatures.EMPTY);
    var filterNoValues =
        new FindAndRerankSort(LegMode.FILTER, null, null, null, null, CommandFeatures.EMPTY);

    assertThat(value1).as("Object equals self").isEqualTo(value1);
    assertThat(value1).as("different vectorize sort").isNotEqualTo(diffVectorize);
    assertThat(value1).as("different lexical sort").isNotEqualTo(diffLexical);
    assertThat(value1).as("different vector").isNotEqualTo(diffVector);
    assertThat(hybridText).as("different hybrid text").isNotEqualTo(diffHybridText);
    assertThat(hybridText)
        .as("hybrid text and hybrid object with the same text")
        .isNotEqualTo(hybridObjectSameText);
    assertThat(hybridNoValues).as("different legMode").isNotEqualTo(filterNoValues);

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
    assertThat(hybridText.hashCode())
        .as("hash code different hybrid text")
        .isNotEqualTo(diffHybridText.hashCode());
    assertThat(hybridText.hashCode())
        .as("hash code hybrid text and hybrid object with the same text")
        .isNotEqualTo(hybridObjectSameText.hashCode());
    assertThat(hybridNoValues.hashCode())
        .as("hash code different legMode")
        .isNotEqualTo(filterNoValues.hashCode());
  }

  @Test
  public void failFilterSortWithValues() {
    assertFilterSortFails(
        () ->
            new FindAndRerankSort(
                LegMode.FILTER, "cheese", null, null, null, CommandFeatures.create()),
        "hybridText must be null when legMode is FILTER, hybridText=cheese");
    assertFilterSortFails(
        () ->
            new FindAndRerankSort(
                LegMode.FILTER, null, "cheese", null, null, CommandFeatures.create()),
        "vectorizeSort must be null when legMode is FILTER, vectorizeSort=cheese");
    assertFilterSortFails(
        () ->
            new FindAndRerankSort(
                LegMode.FILTER, null, null, "cows", null, CommandFeatures.create()),
        "lexicalSort must be null when legMode is FILTER, lexicalSort=cows");
    assertFilterSortFails(
        () ->
            new FindAndRerankSort(
                LegMode.FILTER,
                null,
                null,
                null,
                new float[] {1.1f, 2.2f},
                CommandFeatures.create()),
        "vectorSort must be null when legMode is FILTER, vectorSort.length=2");
  }

  @Test
  public void failHybridTextWithObjectValues() {
    assertHybridTextFails(
        () ->
            new FindAndRerankSort(
                LegMode.HYBRID, "cheese", "cheese", null, null, CommandFeatures.create()));
    assertHybridTextFails(
        () ->
            new FindAndRerankSort(
                LegMode.HYBRID, "cheese", null, "cheese", null, CommandFeatures.create()));
    assertHybridTextFails(
        () ->
            new FindAndRerankSort(
                LegMode.HYBRID,
                "cheese",
                null,
                null,
                new float[] {1.1f, 2.2f},
                CommandFeatures.create()));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("singleLegSortWithOtherValuesCases")
  public void failSingleLegSortWithOtherValues(
      LegMode legMode, Executable newSort, String expectedMessage) {
    var ex = assertThrows(IllegalArgumentException.class, newSort);
    assertThat(ex).hasMessage(expectedMessage);
  }

  private static Stream<Arguments> singleLegSortWithOtherValuesCases() {
    return Stream.of(
        Arguments.of(
            LegMode.VECTORIZE,
            (Executable)
                () ->
                    new FindAndRerankSort(
                        LegMode.VECTORIZE, null, "cheese", "cows", null, CommandFeatures.create()),
            "hybridText, lexicalSort and vectorSort must be null when legMode is VECTORIZE"),
        Arguments.of(
            LegMode.VECTOR,
            (Executable)
                () ->
                    new FindAndRerankSort(
                        LegMode.VECTOR,
                        null,
                        "cheese",
                        null,
                        new float[] {1.1f},
                        CommandFeatures.create()),
            "hybridText, vectorizeSort and lexicalSort must be null when legMode is VECTOR"),
        Arguments.of(
            LegMode.LEXICAL,
            (Executable)
                () ->
                    new FindAndRerankSort(
                        LegMode.LEXICAL,
                        null,
                        null,
                        "cows",
                        new float[] {1.1f},
                        CommandFeatures.create()),
            "hybridText, vectorizeSort and vectorSort must be null when legMode is LEXICAL"));
  }

  @Test
  public void failMissingLegModeOrCommandFeatures() {
    var legModeEx =
        assertThrows(
            NullPointerException.class,
            () -> new FindAndRerankSort(null, null, null, null, null, CommandFeatures.create()));
    assertThat(legModeEx).hasMessage("legMode must not be null");

    var featuresEx =
        assertThrows(
            NullPointerException.class,
            () -> new FindAndRerankSort(LegMode.HYBRID, null, "cheese", null, null, null));
    assertThat(featuresEx).hasMessage("commandFeatures must not be null");
  }

  private static void assertFilterSortFails(Executable newSort, String expectedMessage) {
    var ex = assertThrows(IllegalArgumentException.class, newSort);
    assertThat(ex).hasMessage(expectedMessage);
  }

  private static void assertHybridTextFails(Executable newSort) {
    var ex = assertThrows(IllegalArgumentException.class, newSort);
    assertThat(ex)
        .hasMessage(
            "vectorizeSort, lexicalSort and vectorSort must be null when hybridText is set, hybridText=cheese");
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
            FindAndRerankSort.NO_ARG_SORT),
        Arguments.of(
            """
            null
            """,
            FindAndRerankSort.NO_ARG_SORT),
        // ----
        // $hybrid only
        Arguments.of(
            """
            { "$hybrid" : "same for hybrid and lexical" }
            """,
            new FindAndRerankSort(
                LegMode.HYBRID,
                "same for hybrid and lexical",
                null,
                null,
                null,
                CommandFeatures.of(HYBRID))),
        Arguments.of(
            """
            { "$hybrid" : "" }
            """,
            new FindAndRerankSort(
                LegMode.HYBRID, null, null, null, null, CommandFeatures.of(HYBRID))),
        // ----
        // maximum fields, resolver works out the valid combinations
        Arguments.of(
            """
            { "$hybrid" : { "$vectorize" : "vectorize sort", "$lexical" : "lexical sort", "$vector" : [1.1, 2.2, 3.3]} }
            """,
            new FindAndRerankSort(
                LegMode.HYBRID,
                null,
                "vectorize sort",
                "lexical sort",
                new float[] {1.1f, 2.2f, 3.3f},
                CommandFeatures.of(HYBRID, LEXICAL, VECTOR, VECTORIZE))),
        Arguments.of(
            """
            { "$hybrid" : { "$vectorize" : "vectorize sort", "$lexical" : "lexical sort"} }
            """,
            new FindAndRerankSort(
                LegMode.HYBRID,
                null,
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
                LegMode.HYBRID,
                null,
                "vectorize sort",
                null,
                null,
                CommandFeatures.of(HYBRID, VECTORIZE, LEXICAL))),
        Arguments.of(
            """
            { "$hybrid" : { "$vectorize" : "vectorize sort", "$lexical" : ""} }
            """,
            new FindAndRerankSort(
                LegMode.HYBRID,
                null,
                "vectorize sort",
                null,
                null,
                CommandFeatures.of(HYBRID, VECTORIZE, LEXICAL))),
        Arguments.of(
            """
            { "$hybrid" : { "$vectorize" : "vectorize sort"} }
            """,
            new FindAndRerankSort(
                LegMode.HYBRID,
                null,
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
                LegMode.HYBRID,
                null,
                "vectorize sort",
                "lexical sort",
                null,
                CommandFeatures.of(HYBRID, LEXICAL, VECTORIZE))),
        Arguments.of(
            """
            { "$hybrid" : { "$vectorize" : null, "$lexical" : "lexical sort"} }
            """,
            new FindAndRerankSort(
                LegMode.HYBRID,
                null,
                null,
                "lexical sort",
                null,
                CommandFeatures.of(HYBRID, LEXICAL, VECTORIZE))),
        Arguments.of(
            """
            { "$hybrid" : { "$vectorize" : "", "$lexical" : "lexical sort"} }
            """,
            new FindAndRerankSort(
                LegMode.HYBRID,
                null,
                null,
                "lexical sort",
                null,
                CommandFeatures.of(HYBRID, LEXICAL, VECTORIZE))),
        Arguments.of(
            """
            { "$hybrid" : { "$vectorize" : "   ", "$lexical" : "lexical sort"} }
            """,
            new FindAndRerankSort(
                LegMode.HYBRID,
                null,
                null,
                "lexical sort",
                null,
                CommandFeatures.of(HYBRID, LEXICAL, VECTORIZE))),
        Arguments.of(
            """
            { "$hybrid" : {"$lexical" : "lexical sort"} }
            """,
            new FindAndRerankSort(
                LegMode.HYBRID,
                null,
                null,
                "lexical sort",
                null,
                CommandFeatures.of(HYBRID, LEXICAL))),
        // ----
        // $vector variations
        Arguments.of(
            """
            { "$hybrid" : { "$lexical" : "lexical", "$vector" : null} }
            """,
            new FindAndRerankSort(
                LegMode.HYBRID,
                null,
                null,
                "lexical",
                null,
                CommandFeatures.of(HYBRID, LEXICAL, VECTOR))),
        Arguments.of(
            """
            { "$hybrid" : { "$vectorize" : "vectorize", "$lexical" : "lexical", "$vector" : null} }
            """,
            new FindAndRerankSort(
                LegMode.HYBRID,
                null,
                "vectorize",
                "lexical",
                null,
                CommandFeatures.of(HYBRID, LEXICAL, VECTORIZE, VECTOR))),
        Arguments.of(
            """
            { "$hybrid" : { "$vectorize" : "vectorize", "$lexical" : "lexical", "$vector" : [0.1, 0.2, 0.3]} }
            """,
            new FindAndRerankSort(
                LegMode.HYBRID,
                null,
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
                LegMode.HYBRID,
                null,
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
                LegMode.HYBRID,
                null,
                "vectorize",
                "lexical",
                vector,
                CommandFeatures.of(HYBRID, LEXICAL, VECTORIZE, VECTOR))),
        // ----
        // single leg sorts
        Arguments.of(
            """
            { "$vectorize" : "vectorize sort" }
            """,
            new FindAndRerankSort(
                LegMode.VECTORIZE,
                null,
                "vectorize sort",
                null,
                null,
                CommandFeatures.of(VECTORIZE))),
        Arguments.of(
            """
            { "$vectorize" : "" }
            """,
            new FindAndRerankSort(
                LegMode.VECTORIZE, null, null, null, null, CommandFeatures.of(VECTORIZE))),
        Arguments.of(
            """
            { "$vectorize" : null }
            """,
            new FindAndRerankSort(
                LegMode.VECTORIZE, null, null, null, null, CommandFeatures.of(VECTORIZE))),
        Arguments.of(
            """
            { "$lexical" : "lexical sort" }
            """,
            new FindAndRerankSort(
                LegMode.LEXICAL, null, null, "lexical sort", null, CommandFeatures.of(LEXICAL))),
        Arguments.of(
            """
            { "$lexical" : "   " }
            """,
            new FindAndRerankSort(
                LegMode.LEXICAL, null, null, null, null, CommandFeatures.of(LEXICAL))),
        Arguments.of(
            """
            { "$lexical" : null }
            """,
            new FindAndRerankSort(
                LegMode.LEXICAL, null, null, null, null, CommandFeatures.of(LEXICAL))),
        Arguments.of(
            """
            { "$vector" : [1.1, 2.2, 3.3] }
            """,
            new FindAndRerankSort(
                LegMode.VECTOR,
                null,
                null,
                null,
                new float[] {1.1f, 2.2f, 3.3f},
                CommandFeatures.of(VECTOR))),
        Arguments.of(
                """
            { "$vector" : {"$binary": "%s"} }
            """
                .formatted(vectorBase64),
            new FindAndRerankSort(
                LegMode.VECTOR, null, null, null, vector, CommandFeatures.of(VECTOR))),
        Arguments.of(
            """
            { "$vector" : null }
            """,
            new FindAndRerankSort(
                LegMode.VECTOR, null, null, null, null, CommandFeatures.of(VECTOR))));
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
            "Expected fields: $hybrid, $vectorize, $vector, $lexical. Unexpected fields: fake"),
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
        Arguments.of(
            """
            { "$hybrid" : null}
            """,
            "Field $hybrid may only be of types TextNode, ObjectNode, but got: NullNode"),
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
            "`sort` clause (binary vector) contained fields with the wrong type. Field $vector may only be of types NullNode, ArrayNode, ObjectNode, but got: ObjectNode"),
        // ----
        // single leg sorts
        Arguments.of(
            """
            { "$vectorize" : 1 }
            """,
            "Field $vectorize may only be of types NullNode, TextNode, but got: IntNode"),
        Arguments.of(
            """
            { "$lexical" : 1 }
            """,
            "Field $lexical may only be of types NullNode, TextNode, but got: IntNode"),
        Arguments.of(
            """
            { "$vector" : 1 }
            """,
            "Field $vector may only be of types NullNode, ArrayNode, ObjectNode, but got: IntNode"));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "{ \"$vectorize\" : \"cheese\", \"$lexical\" : \"cows\" }",
        "{ \"$hybrid\" : \"cheese\", \"$vectorize\" : \"cheese\" }"
      })
  public void invalidSortWithTwoTopLevelFields(String jsonString) throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    when(deserializationContext.readTree(jsonParser)).thenReturn(mapper.readTree(jsonString));

    var error =
        assertThrows(
            SortException.class,
            () -> deserializer.deserialize(jsonParser, deserializationContext));
    assertThat(error.code).isEqualTo(SortException.Code.SORT_CLAUSE_INVALID.name());
    assertThat(error.body)
        .contains("sort clause must have exactly one of $hybrid, $vectorize, $vector, $lexical");
  }

  @ParameterizedTest
  @ValueSource(strings = {"{ \"$vector\" : [] }", "{ \"$hybrid\" : { \"$vector\" : [] } }"})
  public void invalidEmptyVector(String jsonString) throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    when(deserializationContext.readTree(jsonParser)).thenReturn(mapper.readTree(jsonString));

    var error =
        assertThrows(
            DocumentException.class,
            () -> deserializer.deserialize(jsonParser, deserializationContext));
    assertThat(error.code).isEqualTo(DocumentException.Code.SHRED_BAD_VECTOR_SIZE.name());
  }
}
