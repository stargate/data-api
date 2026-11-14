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
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.FindAndRerankSort.FilterLeg;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.FindAndRerankSort.HybridObject;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.FindAndRerankSort.HybridText;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.FindAndRerankSort.LexicalLeg;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.FindAndRerankSort.VectorLeg;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression2.FieldSortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression2.LexicalSortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression2.VectorSortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression2.VectorizeSortExpression;
import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.DocumentException;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.SortException;
import io.stargate.sgv2.jsonapi.metrics.CommandFeatures;
import io.stargate.sgv2.jsonapi.util.recordable.PrettyPrintable;
import java.util.List;
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

    var vectorLeg = new VectorLeg(new VectorSortExpression(new float[] {1.1f, 2.2f, 3.3f}));
    var sameVectorLeg = new VectorLeg(new VectorSortExpression(new float[] {1.1f, 2.2f, 3.3f}));
    var diffVectorLeg = new VectorLeg(new VectorSortExpression(new float[] {1.1f, 2.2f, 4.4f}));

    assertThat(value1).as("Object equals self").isEqualTo(value1);
    assertThat(value1).as("same values, vector compared by value").isEqualTo(sameValue);
    assertThat(value1).as("different form").isNotEqualTo(diffForm);
    assertThat(value1).as("different vectorize sort").isNotEqualTo(diffVectorize);
    assertThat(value1).as("different lexical sort").isNotEqualTo(diffLexical);
    assertThat(value1).as("different vector").isNotEqualTo(diffVector);
    assertThat(vectorLeg).as("same $vector, compared by value").isEqualTo(sameVectorLeg);
    assertThat(vectorLeg).as("different $vector").isNotEqualTo(diffVectorLeg);

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
    assertThat(vectorLeg.hashCode())
        .as("hash code same $vector, compared by value")
        .isEqualTo(sameVectorLeg.hashCode());
    assertThat(vectorLeg.hashCode())
        .as("hash code different $vector")
        .isNotEqualTo(diffVectorLeg.hashCode());
  }

  @ParameterizedTest
  @MethodSource({"validSortsTestCases", "singleLegSortsTestCases"})
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

  /** A sort without $hybrid is read into the leg it picks, see {@link #validSorts}. */
  private static Stream<Arguments> singleLegSortsTestCases() {

    var vector = new float[] {1.1f, 2.2f, 3.3f};
    var vectorBase64 = encodeAsMimeBase64(floatsToBytes(vector));

    return Stream.of(
        Arguments.of(
            """
            { "$vectorize" : "cheese" }
            """,
            new VectorLeg(new VectorizeSortExpression("cheese"))),
        Arguments.of(
            """
            { "$vectorize" : "  padded text  " }
            """,
            new VectorLeg(new VectorizeSortExpression("padded text"))),
        Arguments.of(
            """
            { "$vector" : [1.1, 2.2, 3.3] }
            """,
            new VectorLeg(new VectorSortExpression(vector))),
        Arguments.of(
                """
            { "$vector" : {"$binary": "%s"} }
            """
                .formatted(vectorBase64),
            new VectorLeg(new VectorSortExpression(vector))),
        Arguments.of(
            """
            { "$lexical" : "cows" }
            """,
            new LexicalLeg(new LexicalSortExpression("cows"))),
        Arguments.of(
            """
            { "$lexical" : "  padded text  " }
            """,
            new LexicalLeg(new LexicalSortExpression("padded text"))),
        // ----
        // field sorts, in the order sent
        Arguments.of(
            """
            { "age" : -1, "name" : 1 }
            """,
            new FilterLeg(
                List.of(
                    new FieldSortExpression("age", false), new FieldSortExpression("name", true)))),
        Arguments.of(
            """
            { "name" : 1, "age" : -1 }
            """,
            new FilterLeg(
                List.of(
                    new FieldSortExpression("name", true), new FieldSortExpression("age", false)))),
        Arguments.of(
            """
            { "address.city" : 1 }
            """,
            new FilterLeg(List.of(new FieldSortExpression("address.city", true)))),
        Arguments.of(
            """
            { " " : 1 }
            """,
            new FilterLeg(List.of(new FieldSortExpression(" ", true)))));
  }

  /**
   * A single leg sort works out its metrics features from its type. The expected features are
   * written out here, so the test does not use the code it tests.
   */
  @ParameterizedTest
  @MethodSource("singleLegFeaturesTestCases")
  public void singleLegFeatures(String jsonString, CommandFeatures expected) throws Exception {

    LOGGER.debug("singleLegFeatures() - jsonString: {}", jsonString);
    var mapper = new ObjectMapper();
    when(deserializationContext.readTree(jsonParser)).thenReturn(mapper.readTree(jsonString));

    var actual = deserializer.deserialize(jsonParser, deserializationContext);

    assertEquals(expected, actual.commandFeatures());
  }

  private static Stream<Arguments> singleLegFeaturesTestCases() {

    var vectorBase64 = encodeAsMimeBase64(floatsToBytes(new float[] {1.1f, 2.2f, 3.3f}));

    return Stream.of(
        Arguments.of(
            """
            { "$vectorize" : "cheese" }
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
            { "$lexical" : "cows" }
            """,
            CommandFeatures.of(LEXICAL)),
        Arguments.of(
            """
            { "age" : -1, "name" : 1 }
            """,
            CommandFeatures.EMPTY),
        Arguments.of(
            """
            {}
            """,
            CommandFeatures.EMPTY),
        Arguments.of(
            """
            null
            """,
            CommandFeatures.EMPTY));
  }

  /** The error for each syntax mistake in a sort without $hybrid. */
  @ParameterizedTest
  @MethodSource("invalidSingleLegSortsTestCases")
  public void invalidSingleLegSorts(
      String jsonString,
      Class<? extends APIException> exceptionClass,
      ErrorCode<?> code,
      String message)
      throws Exception {

    LOGGER.debug("invalidSingleLegSorts() - jsonString: {}", jsonString);
    var mapper = new ObjectMapper();
    when(deserializationContext.readTree(jsonParser)).thenReturn(mapper.readTree(jsonString));

    var error =
        assertThrowsExactly(
            exceptionClass, () -> deserializer.deserialize(jsonParser, deserializationContext));
    LOGGER.debug("invalidSingleLegSorts() - error.getMessage(): {}", error.getMessage());

    assertThat(error.code).isEqualTo(code.name());
    assertThat(error).hasMessageContaining(message);
  }

  private static Stream<Arguments> invalidSingleLegSortsTestCases() {
    return Stream.of(
        // ----
        // $lexical, $vector or $vectorize with another field, checked in the same order as find
        Arguments.of(
            """
            { "$lexical" : "cows", "age" : 1 }
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_INVALID,
            "when sorting by field '$lexical' no other sort expressions allowed"),
        Arguments.of(
            """
            { "age" : 1, "$vector" : [1.1, 2.2, 3.3] }
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_INVALID,
            "when sorting by field '$vector' no other sort expressions allowed"),
        Arguments.of(
            """
            { "$vectorize" : "cheese", "age" : 1 }
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_INVALID,
            "when sorting by field '$vectorize' no other sort expressions allowed"),
        Arguments.of(
            """
            { "$vectorize" : "cheese", "$lexical" : "cows" }
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_INVALID,
            "when sorting by field '$lexical' no other sort expressions allowed"),
        Arguments.of(
            """
            { "$vectorize" : "cheese", "$vector" : [1.1, 2.2, 3.3] }
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_INVALID,
            "when sorting by field '$vector' no other sort expressions allowed"),
        // ----
        // $lexical and $vectorize values: non-blank text
        Arguments.of(
            """
            { "$lexical" : null }
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_VALUE_INVALID,
            "on path '$lexical' not valid: value must be a non-blank String, not null"),
        Arguments.of(
            """
            { "$lexical" : "   " }
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_VALUE_INVALID,
            "value must be a non-blank String, not \"   \""),
        Arguments.of(
            """
            { "$lexical" : "" }
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_VALUE_INVALID,
            "value must be a non-blank String, not \"\""),
        Arguments.of(
            """
            { "$lexical" : 1 }
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_VALUE_INVALID,
            "value must be a non-blank String, not 1"),
        // text that is only blank after it is trimmed: a control character is removed by trim()
        Arguments.of(
            """
            { "$lexical" : "\\u0001" }
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_VALUE_INVALID,
            "on path '$lexical' not valid: value must be a non-blank String"),
        Arguments.of(
            """
            { "$vectorize" : "\\u0001\\u2003" }
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_VALUE_INVALID,
            "on path '$vectorize' not valid: value must be a non-blank String"),
        Arguments.of(
            """
            { "$vectorize" : null }
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_VALUE_INVALID,
            "on path '$vectorize' not valid: value must be a non-blank String, not null"),
        Arguments.of(
            """
            { "$vectorize" : "" }
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_VALUE_INVALID,
            "value must be a non-blank String, not \"\""),
        Arguments.of(
            """
            { "$vectorize" : ["cheese"] }
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_VALUE_INVALID,
            "value must be a non-blank String, not [\"cheese\"]"),
        // ----
        // $vector values: an array of numbers or $binary
        Arguments.of(
            """
            { "$vector" : null }
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_VALUE_INVALID,
            "on path '$vector' not valid: value must be an Array of numbers or a $binary vector, not Null"),
        Arguments.of(
            """
            { "$vector" : "cheese" }
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_VALUE_INVALID,
            "value must be an Array of numbers or a $binary vector, not String"),
        Arguments.of(
            """
            { "$vector" : {"$date": 1} }
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_VALUE_INVALID,
            "only binary vector object values are supported for sorting"),
        Arguments.of(
            """
            { "$vector" : {"$binary": "not base64!"} }
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_VALUE_INVALID,
            "Invalid content in EJSON $binary wrapper"),
        Arguments.of(
            """
            { "$vector" : [] }
            """,
            DocumentException.class,
            DocumentException.Code.SHRED_BAD_VECTOR_SIZE,
            "cannot be empty Array"),
        Arguments.of(
            """
            { "$vector" : [1.1, "cheese"] }
            """,
            DocumentException.class,
            DocumentException.Code.SHRED_BAD_VECTOR_VALUE,
            "has a String value (\"cheese\")"),
        // ----
        // field sort paths, every path is checked before any value, the same as find
        Arguments.of(
            """
            { "$fake" : 1 }
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_PATH_INVALID,
            "Path '$fake' used in sort clause not valid: path cannot start with '$'"),
        Arguments.of(
            """
            { "age" : 1, "$fake" : 1 }
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_PATH_INVALID,
            "Path '$fake' used in sort clause not valid"),
        Arguments.of(
            """
            { "age" : 2, "$fake" : 1 }
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_PATH_INVALID,
            "Path '$fake' used in sort clause not valid"),
        // a wrong spelling of $hybrid is not $hybrid, so it is a field sort with a path that is not
        // valid
        Arguments.of(
            """
            { "$hybird" : "cheese" }
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_PATH_INVALID,
            "Path '$hybird' used in sort clause not valid"),
        Arguments.of(
            """
            { "" : 1 }
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_PATH_INVALID,
            "path must be represented as a non-empty string"),
        Arguments.of(
            """
            { "a&b" : 1 }
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_PATH_INVALID,
            "The ampersand character '&' at position 1 must be followed by either '&' or '.'"),
        // ----
        // field sort values, only 1 and -1, the same as find
        Arguments.of(
            """
            { "age" : 0 }
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_VALUE_INVALID,
            "sort ordering value can only be `1` for ascending or `-1` for descending (not `0`)"),
        Arguments.of(
            """
            { "age" : 2 }
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_VALUE_INVALID,
            "(not `2`)"),
        Arguments.of(
            """
            { "age" : 1, "name" : -2 }
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_VALUE_INVALID,
            "on path 'name' not valid"),
        Arguments.of(
            """
            { "age" : 1.0 }
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_VALUE_INVALID,
            "sort ordering value should be integer `1` or `-1`; or Array (vector); or String (lexical or vectorize), was: Number"),
        Arguments.of(
            """
            { "age" : null }
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_VALUE_INVALID,
            "was: Null"),
        Arguments.of(
            """
            { "age" : "asc" }
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_VALUE_INVALID,
            "sort ordering value can be String only for lexical or vectorize search"),
        Arguments.of(
            """
            { "age" : [1] }
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_VALUE_INVALID,
            "sort ordering value can be Array only for vector search"));
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
        // a null $hybrid is still a $hybrid sort, not a single leg sort
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
            "`sort` clause (binary vector) contained fields with the wrong type. Field $vector may only be of types NullNode, ArrayNode, ObjectNode, but got: ObjectNode"));
  }
}
