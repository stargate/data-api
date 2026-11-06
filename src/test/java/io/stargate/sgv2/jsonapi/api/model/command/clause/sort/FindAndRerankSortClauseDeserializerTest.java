package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

import static io.stargate.sgv2.jsonapi.api.model.command.clause.sort.FindAndRerankSort.*;
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
            new UnresolvedSingleLeg(new ObjectMapper().createObjectNode(), CommandFeatures.EMPTY)),
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
            { "$hybrid" : { "$vectorize" : "   ", "$lexical" : "lexical sort"} }
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
            { "$hybrid" : { "$lexical" : "lexical", "$vector" : null} }
            """,
            new HybridObject(null, "lexical", null, CommandFeatures.of(HYBRID, LEXICAL, VECTOR))),
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
            { "$hybrid" : "cheese", "age" : -1 }
            """,
            "Expected fields: $hybrid, $vectorize, $vector, $lexical. Unexpected fields: age"),
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

  @ParameterizedTest
  @ValueSource(
      strings = {
        "{ \"$hybrid\" : \"cheese\", \"$vectorize\" : \"cheese\" }",
        "{ \"$hybrid\" : \"cheese\", \"$lexical\" : \"cows\" }"
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
  @ValueSource(strings = {"{ \"$hybrid\" : { \"$vector\" : [] } }"})
  public void invalidEmptyVector(String jsonString) throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    when(deserializationContext.readTree(jsonParser)).thenReturn(mapper.readTree(jsonString));

    var error =
        assertThrows(
            DocumentException.class,
            () -> deserializer.deserialize(jsonParser, deserializationContext));
    assertThat(error.code).isEqualTo(DocumentException.Code.SHRED_BAD_VECTOR_SIZE.name());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "{}",
        "{\"age\": -1}",
        "{\"age\": 1, \"name\": -1}",
        "{\"$lexical\": \"  cows  \"}",
        "{\"$lexical\": \"   \"}",
        "{\"$lexical\": null}",
        "{\"$lexical\": 1}",
        "{\"$lexical\": \"cows\", \"age\": -1}",
        "{\"$vectorize\": \"  cheese  \"}",
        "{\"$vectorize\": null}",
        "{\"$vectorize\": 1}",
        "{\"$vector\": [1, 2, 3]}",
        "{\"$vector\": []}",
        "{\"$vector\": null}",
        "{\"$vector\": {\"$binary\": \"P4AAAEAAAABAQAAA\"}}",
        "{\"$vector\": [1, 2, 3], \"$vectorize\": \"cheese\"}",
        "{\"age\": 2}",
        "{\"$unknown\": 1}"
      })
  void preservesSingleLegSortUntilPlanning(String jsonString) throws Exception {
    var rawSort = new ObjectMapper().readTree(jsonString);
    when(deserializationContext.readTree(jsonParser)).thenReturn(rawSort);

    var actual = deserializer.deserialize(jsonParser, deserializationContext);

    assertThat(actual).isInstanceOf(UnresolvedSingleLeg.class);
    assertThat(((UnresolvedSingleLeg) actual).sort()).isEqualTo(rawSort);
  }

  @Test
  void distinguishesExplicitNullFromEmptySingleLegSort() throws Exception {
    var mapper = new ObjectMapper();
    when(deserializationContext.readTree(jsonParser))
        .thenReturn(mapper.readTree("{\"$lexical\": null}"), mapper.readTree("{}"));

    var explicitNull = deserializer.deserialize(jsonParser, deserializationContext);
    var empty = deserializer.deserialize(jsonParser, deserializationContext);

    assertThat(explicitNull).isNotEqualTo(empty);
    assertThat(explicitNull.commandFeatures()).isEqualTo(CommandFeatures.of(LEXICAL));
    assertThat(empty.commandFeatures()).isEqualTo(CommandFeatures.EMPTY);
  }
}
