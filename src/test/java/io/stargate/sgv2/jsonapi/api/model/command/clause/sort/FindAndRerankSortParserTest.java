package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

import static io.stargate.sgv2.jsonapi.util.Base64Util.encodeAsMimeBase64;
import static io.stargate.sgv2.jsonapi.util.CqlVectorUtil.floatsToBytes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
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
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@link FindAndRerankSortParser}: the sort each form of sort parses into, and the error
 * for each syntax mistake.
 */
class FindAndRerankSortParserTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** The vector in the $vector cases, sent as an array or as $binary. */
  private static final float[] VECTOR = {1.1f, 2.2f, 3.3f};

  private static final String VECTOR_BASE64 = encodeAsMimeBase64(floatsToBytes(VECTOR));

  // ===========================================================================================
  // No sort, and a sort that is not an object
  // ===========================================================================================

  /** No sort, a null sort and an empty sort are the filter leg, without sorting. */
  @Test
  void parsesNoSortToFilterLegWithoutSorts() throws Exception {
    var noSorts = new FilterLeg(List.of());

    assertThat(FindAndRerankSortParser.parse(FindAndRerankSortDefinition.NO_ARG_SORT))
        .as("no sort")
        .isEqualTo(noSorts);
    assertThat(
            FindAndRerankSortParser.parse(new FindAndRerankSortDefinition(NullNode.getInstance())))
        .as("null sort")
        .isEqualTo(noSorts);
    assertThat(parse("{}")).as("empty sort").isEqualTo(noSorts);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("notObjectCases")
  void rejectsSortThatIsNotAnObject(String sort, String nodeType) {
    var ex = assertThrowsExactly(SortException.class, () -> parse(sort));

    assertThat(ex.code).isEqualTo(SortException.Code.SORT_CLAUSE_INVALID.name());
    assertThat(ex)
        .hasMessageContaining("sort clause must be submitted as JSON Object, not " + nodeType);
  }

  static Stream<Arguments> notObjectCases() {
    return Stream.of(
        Arguments.of("\"cheese\"", "String"),
        Arguments.of("1", "Number"),
        Arguments.of("[1, 2]", "Array"));
  }

  // ===========================================================================================
  // $hybrid
  // ===========================================================================================

  @ParameterizedTest(name = "{0}")
  @MethodSource("hybridCases")
  void parsesHybridSort(String sort, FindAndRerankSort expected) throws Exception {
    assertThat(parse(sort)).isEqualTo(expected);
  }

  static Stream<Arguments> hybridCases() {
    return Stream.of(
        Arguments.of(
            """
            {"$hybrid": "cheese"}
            """,
            new HybridText("cheese")),
        Arguments.of(
            """
            {"$hybrid": "  padded text  "}
            """,
            new HybridText("padded text")),
        // ----
        // $hybrid object, values that are null or blank are left out
        Arguments.of(
            """
            {"$hybrid": {"$vectorize": "cheese", "$lexical": "cows", "$vector": [1.1, 2.2, 3.3]}}
            """,
            new HybridObject("cheese", "cows", VECTOR)),
        Arguments.of(
            """
            {"$hybrid": {"$vectorize": "cheese", "$lexical": "cows"}}
            """,
            new HybridObject("cheese", "cows", null)),
        Arguments.of(
            """
            {"$hybrid": {"$vectorize": "  padded text  "}}
            """,
            new HybridObject("padded text", null, null)),
        Arguments.of(
            """
            {"$hybrid": {"$vectorize": "cheese", "$lexical": null, "$vector": null}}
            """,
            new HybridObject("cheese", null, null)),
        Arguments.of(
            """
            {"$hybrid": {"$vectorize": "cheese", "$lexical": "   "}}
            """,
            new HybridObject("cheese", null, null)),
        Arguments.of(
            """
            {"$hybrid": {"$vector": [1.1, 2.2, 3.3], "$lexical": "cows"}}
            """,
            new HybridObject(null, "cows", VECTOR)),
        Arguments.of(
            """
            {"$hybrid": {"$vectorize": null, "$vector": [1.1, 2.2, 3.3]}}
            """,
            new HybridObject(null, null, VECTOR)),
        Arguments.of(
                """
            {"$hybrid": {"$vector": {"$binary": "%s"}}}
            """
                .formatted(VECTOR_BASE64),
            new HybridObject(null, null, VECTOR)));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("invalidHybridCases")
  void rejectsHybridSort(
      String sort,
      Class<? extends APIException> exceptionClass,
      ErrorCode<?> code,
      String message) {
    assertRejected(sort, exceptionClass, code, message);
  }

  static Stream<Arguments> invalidHybridCases() {
    return Stream.of(
        // ----
        // structure
        Arguments.of(
            """
            {"$hybrid": "cheese", "age": 1}
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_INVALID,
            "when sorting by field '$hybrid' no other sort expressions allowed"),
        Arguments.of(
            """
            {"$hybrid": {"$vectorize": "cheese", "fake": 1}}
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_INVALID,
            "a $hybrid object can only have $vectorize, $vector and $lexical, not 'fake'"),
        // ----
        // value types
        Arguments.of(
            """
            {"$hybrid": 1}
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_VALUE_INVALID,
            "on path '$hybrid' not valid: value must be a String or an Object, not Number"),
        Arguments.of(
            """
            {"$hybrid": null}
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_VALUE_INVALID,
            "value must be a String or an Object, not Null"),
        Arguments.of(
            """
            {"$hybrid": {"$vectorize": 1}}
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_VALUE_INVALID,
            "on path '$hybrid.$vectorize' not valid: value must be a String or null, not Number"),
        Arguments.of(
            """
            {"$hybrid": {"$vectorize": "cheese", "$lexical": ["cows"]}}
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_VALUE_INVALID,
            "on path '$hybrid.$lexical' not valid: value must be a String or null, not Array"),
        Arguments.of(
            """
            {"$hybrid": {"$vector": "cheese"}}
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_VALUE_INVALID,
            "on path '$hybrid.$vector' not valid: value must be an Array of numbers or a $binary vector, not String"),
        Arguments.of(
            """
            {"$hybrid": {"$vector": {"$date": 1}}}
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_VALUE_INVALID,
            "only binary vector object values are supported for sorting"),
        Arguments.of(
            """
            {"$hybrid": {"$vector": []}}
            """,
            DocumentException.class,
            DocumentException.Code.SHRED_BAD_VECTOR_SIZE,
            "cannot be empty Array"));
  }

  /** A $hybrid sort must have a value for the vector leg. */
  @ParameterizedTest(name = "{0}")
  @MethodSource("hybridWithNoValueCases")
  void rejectsHybridSortWithNoValue(String sort, SortException.Code code) {
    var ex = assertThrowsExactly(SortException.class, () -> parse(sort));

    assertThat(ex.code).isEqualTo(code.name());
  }

  static Stream<Arguments> hybridWithNoValueCases() {
    return Stream.of(
        Arguments.of(
            """
            {"$hybrid": ""}
            """,
            SortException.Code.MISSING_HYBRID_SORT_VALUE),
        Arguments.of(
            """
            {"$hybrid": "   "}
            """,
            SortException.Code.MISSING_HYBRID_SORT_VALUE),
        Arguments.of(
            """
            {"$hybrid": {}}
            """,
            SortException.Code.MISSING_HYBRID_SORT_VALUE),
        Arguments.of(
            """
            {"$hybrid": {"$vectorize": null}}
            """,
            SortException.Code.MISSING_HYBRID_SORT_VALUE),
        Arguments.of(
            """
            {"$hybrid": {"$vector": null}}
            """,
            SortException.Code.MISSING_HYBRID_SORT_VALUE),
        Arguments.of(
            """
            {"$hybrid": {"$lexical": null}}
            """,
            SortException.Code.MISSING_HYBRID_SORT_VALUE),
        Arguments.of(
            """
            {"$hybrid": {"$lexical": ""}}
            """,
            SortException.Code.MISSING_HYBRID_SORT_VALUE),
        Arguments.of(
            """
            {"$hybrid": {"$lexical": "cows"}}
            """,
            SortException.Code.MISSING_VECTOR_OR_VECTORIZE_IN_HYBRID_SORT),
        Arguments.of(
            """
            {"$hybrid": {"$lexical": "cows", "$vectorize": null}}
            """,
            SortException.Code.MISSING_VECTOR_OR_VECTORIZE_IN_HYBRID_SORT),
        Arguments.of(
            """
            {"$hybrid": {"$lexical": "cows", "$vectorize": "   "}}
            """,
            SortException.Code.MISSING_VECTOR_OR_VECTORIZE_IN_HYBRID_SORT),
        Arguments.of(
            """
            {"$hybrid": {"$lexical": "cows", "$vector": null}}
            """,
            SortException.Code.MISSING_VECTOR_OR_VECTORIZE_IN_HYBRID_SORT));
  }

  // ===========================================================================================
  // Single leg sorts
  // ===========================================================================================

  @ParameterizedTest(name = "{0}")
  @MethodSource("singleLegCases")
  void parsesSingleLegSort(String sort, FindAndRerankSort expected) throws Exception {
    assertThat(parse(sort)).isEqualTo(expected);
  }

  static Stream<Arguments> singleLegCases() {
    return Stream.of(
        Arguments.of(
            """
            {"$vectorize": "cheese"}
            """,
            new VectorLeg(new VectorizeSortExpression("cheese"))),
        Arguments.of(
            """
            {"$vectorize": "  padded text  "}
            """,
            new VectorLeg(new VectorizeSortExpression("padded text"))),
        Arguments.of(
            """
            {"$vector": [1.1, 2.2, 3.3]}
            """,
            new VectorLeg(new VectorSortExpression(VECTOR))),
        Arguments.of(
                """
            {"$vector": {"$binary": "%s"}}
            """
                .formatted(VECTOR_BASE64),
            new VectorLeg(new VectorSortExpression(VECTOR))),
        Arguments.of(
            """
            {"$lexical": "cows"}
            """,
            new LexicalLeg(new LexicalSortExpression("cows"))),
        Arguments.of(
            """
            {"$lexical": "  padded text  "}
            """,
            new LexicalLeg(new LexicalSortExpression("padded text"))),
        // ----
        // field sorts, in the order sent
        Arguments.of(
            """
            {"age": -1, "name": 1}
            """,
            new FilterLeg(
                List.of(
                    new FieldSortExpression("age", false), new FieldSortExpression("name", true)))),
        Arguments.of(
            """
            {"name": 1, "age": -1}
            """,
            new FilterLeg(
                List.of(
                    new FieldSortExpression("name", true), new FieldSortExpression("age", false)))),
        Arguments.of(
            """
            {"address.city": 1}
            """,
            new FilterLeg(List.of(new FieldSortExpression("address.city", true)))),
        Arguments.of(
            """
            {" ": 1}
            """,
            new FilterLeg(List.of(new FieldSortExpression(" ", true)))));
  }

  /** A single leg sort gets the same error codes as find for the same mistakes. */
  @ParameterizedTest(name = "{0}")
  @MethodSource("invalidSingleLegCases")
  void rejectsSingleLegSort(
      String sort,
      Class<? extends APIException> exceptionClass,
      ErrorCode<?> code,
      String message) {
    assertRejected(sort, exceptionClass, code, message);
  }

  static Stream<Arguments> invalidSingleLegCases() {
    return Stream.of(
        // ----
        // $lexical, $vector or $vectorize with another field, checked in the same order as find
        Arguments.of(
            """
            {"$lexical": "cows", "age": 1}
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_INVALID,
            "when sorting by field '$lexical' no other sort expressions allowed"),
        Arguments.of(
            """
            {"age": 1, "$vector": [1.1, 2.2, 3.3]}
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_INVALID,
            "when sorting by field '$vector' no other sort expressions allowed"),
        Arguments.of(
            """
            {"$vectorize": "cheese", "age": 1}
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_INVALID,
            "when sorting by field '$vectorize' no other sort expressions allowed"),
        Arguments.of(
            """
            {"$vectorize": "cheese", "$lexical": "cows"}
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_INVALID,
            "when sorting by field '$lexical' no other sort expressions allowed"),
        Arguments.of(
            """
            {"$vectorize": "cheese", "$vector": [1.1, 2.2, 3.3]}
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_INVALID,
            "when sorting by field '$vector' no other sort expressions allowed"),
        // ----
        // $lexical and $vectorize values: non-blank text
        Arguments.of(
            """
            {"$lexical": null}
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_VALUE_INVALID,
            "on path '$lexical' not valid: value must be a non-blank String, not null"),
        Arguments.of(
            """
            {"$lexical": "   "}
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_VALUE_INVALID,
            "value must be a non-blank String, not \"   \""),
        Arguments.of(
            """
            {"$lexical": 1}
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_VALUE_INVALID,
            "value must be a non-blank String, not 1"),
        Arguments.of(
            """
            {"$vectorize": null}
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_VALUE_INVALID,
            "on path '$vectorize' not valid: value must be a non-blank String, not null"),
        Arguments.of(
            """
            {"$vectorize": ""}
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_VALUE_INVALID,
            "value must be a non-blank String, not \"\""),
        Arguments.of(
            """
            {"$vectorize": ["cheese"]}
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_VALUE_INVALID,
            "value must be a non-blank String, not [\"cheese\"]"),
        // ----
        // $vector values: an array of numbers or $binary
        Arguments.of(
            """
            {"$vector": null}
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_VALUE_INVALID,
            "on path '$vector' not valid: value must be an Array of numbers or a $binary vector, not Null"),
        Arguments.of(
            """
            {"$vector": "cheese"}
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_VALUE_INVALID,
            "value must be an Array of numbers or a $binary vector, not String"),
        Arguments.of(
            """
            {"$vector": {"$date": 1}}
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_VALUE_INVALID,
            "only binary vector object values are supported for sorting"),
        Arguments.of(
            """
            {"$vector": {"$binary": "not base64!"}}
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_VALUE_INVALID,
            "Invalid content in EJSON $binary wrapper"),
        Arguments.of(
            """
            {"$vector": []}
            """,
            DocumentException.class,
            DocumentException.Code.SHRED_BAD_VECTOR_SIZE,
            "cannot be empty Array"),
        Arguments.of(
            """
            {"$vector": [1.1, "cheese"]}
            """,
            DocumentException.class,
            DocumentException.Code.SHRED_BAD_VECTOR_VALUE,
            "has a String value (\"cheese\")"),
        // ----
        // field sort paths, every path is checked before any value, the same as find
        Arguments.of(
            """
            {"$fake": 1}
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_PATH_INVALID,
            "Path '$fake' used in sort clause not valid: path cannot start with '$'"),
        Arguments.of(
            """
            {"age": 1, "$fake": 1}
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_PATH_INVALID,
            "Path '$fake' used in sort clause not valid"),
        Arguments.of(
            """
            {"age": 2, "$fake": 1}
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_PATH_INVALID,
            "Path '$fake' used in sort clause not valid"),
        Arguments.of(
            """
            {"": 1}
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_PATH_INVALID,
            "path must be represented as a non-empty string"),
        Arguments.of(
            """
            {"a&b": 1}
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_PATH_INVALID,
            "The ampersand character '&' at position 1 must be followed by either '&' or '.'"),
        // ----
        // field sort values, only 1 and -1, the same as find
        Arguments.of(
            """
            {"age": 0}
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_VALUE_INVALID,
            "sort ordering value can only be `1` for ascending or `-1` for descending (not `0`)"),
        Arguments.of(
            """
            {"age": 2}
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_VALUE_INVALID,
            "(not `2`)"),
        Arguments.of(
            """
            {"age": 1, "name": -2}
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_VALUE_INVALID,
            "on path 'name' not valid"),
        Arguments.of(
            """
            {"age": 1.0}
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_VALUE_INVALID,
            "sort ordering value should be integer `1` or `-1`; or Array (vector); or String (lexical or vectorize), was: Number"),
        Arguments.of(
            """
            {"age": null}
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_VALUE_INVALID,
            "was: Null"),
        Arguments.of(
            """
            {"age": "asc"}
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_VALUE_INVALID,
            "sort ordering value can be String only for lexical or vectorize search"),
        Arguments.of(
            """
            {"age": [1]}
            """,
            SortException.class,
            SortException.Code.SORT_CLAUSE_VALUE_INVALID,
            "sort ordering value can be Array only for vector search"));
  }

  // ===========================================================================================
  // Helpers / no more tests below
  // ===========================================================================================

  /** Parses the sort JSON, as the command keeps it. */
  private static FindAndRerankSort parse(String sort) throws JsonProcessingException {
    return FindAndRerankSortParser.parse(new FindAndRerankSortDefinition(MAPPER.readTree(sort)));
  }

  /** Asserts parsing fails with the exception, the code and a message that has the text. */
  private static void assertRejected(
      String sort,
      Class<? extends APIException> exceptionClass,
      ErrorCode<?> code,
      String message) {
    var ex = assertThrowsExactly(exceptionClass, () -> parse(sort));

    assertThat(ex.code).isEqualTo(code.name());
    assertThat(ex).hasMessageContaining(message);
  }
}
