package io.stargate.sgv2.jsonapi.api.v1.findandrerank;

import static io.stargate.sgv2.jsonapi.api.v1.AbstractKeyspaceIntegrationTestBase.TEST_PROP_LEXICAL_DISABLED;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankAssertions.assertApiError;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankAssertions.assertIds;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankAssertions.assertNoDollarFields;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankAssertions.assertRuntimeError;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankAssertions.assertScores;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankAssertions.assertSortVector;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankAssertions.rrf;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankAssertions.scores;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.BYO;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.EXAMPLE_VECTOR;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.IDS;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.IDS_TERM;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.IDS_VECTOR;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.MAIN;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.NOLEX;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankRequests.findAndRerank;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.RerankerAssertions.assertRerankerCalls;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.RerankerAssertions.assertRerankerNotCalled;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.RerankerAssertions.defaultModelCalls;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.nullValue;

import com.fasterxml.jackson.databind.JsonNode;
import io.restassured.response.ValidatableResponse;
import io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankAssertions.ExpectedScores;
import io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.Fixture;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.RequestException;
import io.stargate.sgv2.jsonapi.exception.ServerException;
import io.stargate.sgv2.jsonapi.exception.UpdateException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * findAndRerank tests about {@code options}: limit, hybridLimits, rerankQuery, rerankOn,
 * includeScores and includeSortVector, each missing, valid, of the wrong type and out of range.
 *
 * <p>Notes for review: most tests send {@link #OPTION_SORT} ($vectorize only) to {@link
 * FindAndRerankFixtures#MAIN}, so only the vector read runs and HCD and DSE agree; expected orders
 * follow the passage scores in {@link FakeRerankerScores}. Option JSON uses single quotes. Read
 * limits come from the {@code LIMIT n} of the ANN and BM25 (HCD only) reads in {@code
 * status.trace}; the test configuration sets the hybridLimits default to 5, which main ignores.
 */
public interface FindAndRerankOptionCases extends FindAndRerankTestContext {

  String OPTION_CASES = "io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankOptionCases#";
  // Query text of OPTION_SORT; the test embedding provider has its own vector for it.
  String OPTION_QUERY = "ChatGPT upgraded";
  // Sort with only a $vectorize text, so no BM25 read runs on either backend.
  String OPTION_SORT = "{\"$hybrid\": {\"$vectorize\": \"" + OPTION_QUERY + "\"}}";
  // $hybrid text of the hybridLimits tests; on HCD the BM25 read finds m01 to m05.
  String OPTION_HYBRID_TEXT = "A tree in the woods";
  // Keeps m01 to m05 of the main collection; their vector and BM25 orders have no ties.
  String OPTION_DISTINCT = "{\"grp\": \"distinct\"}";
  // The twelve main documents with $vectorize, in rerank order of that text.
  List<String> ALL_RANKED =
      List.of("m08", "m05", "m11", "m03", "m06", "m02", "m12", "m04", "m09", "m01", "m07", "m10");
  // m01 to m05 in rerank order of their $vectorize text.
  List<String> DISTINCT_RANKED = List.of("m05", "m03", "m02", "m04", "m01");

  // The docs (Properties of options) default hybridLimits to limit and rerankOn to $lexical; main
  // reads 50, reranks the $vectorize text of all 12 documents and returns the top limit (default
  // 10). The test pins current behavior; whether it is a bug is still under discussion.
  @ParameterizedTest(name = "{0}")
  @MethodSource(OPTION_CASES + "limitCases")
  default void limitDefaultsToTenAndCutsOnlyAfterReranking(LimitCase c) {
    assertLimitCase(c);
  }

  static Stream<Named<LimitCase>> limitCases() {
    return Stream.of(
        limit("options missing -> 10 of 12 returned", null, 10),
        limit("options is null -> 10 of 12 returned", "null", 10),
        limit("options is empty -> 10 of 12 returned", "{}", 10),
        limit("limit is null -> 10 of 12 returned", "{'limit': null}", 10),
        limit("limit is 3 -> all 12 reranked, 3 returned", "{'limit': 3}", 3),
        limit("limit is 50 -> all 12 returned", "{'limit': 50}", 12));
  }

  // limit 10000 and the int maximum return all 12 documents. The docs do not specify an upper
  // bound for limit; the test pins current behavior so that any change is visible in review.
  @ParameterizedTest(name = "{0}")
  @MethodSource(OPTION_CASES + "largeLimitCases")
  default void limitHasNoUpperBound(LimitCase c) {
    assertLimitCase(c);
  }

  static Stream<Named<LimitCase>> largeLimitCases() {
    return Stream.of(
        limit("limit is 10000 -> all 12 returned", "{'limit': 10000}", 12),
        limit("limit is the int maximum -> all 12 returned", "{'limit': 2147483647}", 12));
  }

  // limit 5.9 and "5" become 5; "", blank and "null" give the default 10. The docs (Properties of
  // options, limit) give limit the type integer; main converts these values instead of rejecting
  // them. The test pins current behavior; whether it is a bug is still under discussion.
  @ParameterizedTest(name = "{0}")
  @MethodSource(OPTION_CASES + "convertedLimitCases")
  default void limitDecimalOrStringIsConverted(LimitCase c) {
    assertLimitCase(c);
  }

  static Stream<Named<LimitCase>> convertedLimitCases() {
    return Stream.of(
        limit("limit is 5.9 -> truncated to 5", "{'limit': 5.9}", 5),
        limit("limit is the string 5 -> 5", "{'limit': '5'}", 5),
        limit("limit is an empty string -> default 10", "{'limit': ''}", 10),
        limit("limit is a blank string -> default 10", "{'limit': '   '}", 10),
        limit("limit is the string null -> default 10", "{'limit': 'null'}", 10));
  }

  // The docs (Properties of options, hybridLimits) say the values are integers and default to
  // limit; with limit 10, main reads 50 per read when hybridLimits is missing or null (not the
  // configured 5), truncates decimals and wraps numbers beyond the int range. The test pins current
  // behavior; whether it is a bug is still under discussion.
  @ParameterizedTest(name = "{0}")
  @MethodSource(OPTION_CASES + "defaultOrTruncatedHybridLimitsCases")
  default void hybridLimitsDefaultsToFiftyAndTruncatesNumbers(HybridLimitsCase c) {
    assertHybridLimitsCase(c);
  }

  static Stream<Named<HybridLimitsCase>> defaultOrTruncatedHybridLimitsCases() {
    String object = "{'$vector':10.9,'$lexical':4294967306}";
    return Stream.of(
        hybridLimits("hybridLimits missing -> LIMIT 50 and 50", null, 50, 50),
        hybridLimits("hybridLimits is null -> LIMIT 50 and 50", "null", 50, 50),
        hybridLimits("hybridLimits is 50.7 -> LIMIT 50", "50.7", 50, 50),
        hybridLimits("hybridLimits is 4294967346 -> wraps to LIMIT 50", "4294967346", 50, 50),
        hybridLimits("object with 10.9 and 4294967306 -> LIMIT 10 and 10", object, 10, 10));
  }

  // hybridLimits as a number sets both read limits and as an object sets each one; the result has
  // exactly the documents those reads found, with no warning even when fewer than limit 10. Per the
  // findAndRerank docs (Properties of options, hybridLimits). The docs give no bounds and do not
  // cover hybridLimits below limit; those cases pin current behavior so any change is visible.
  @ParameterizedTest(name = "{0}")
  @MethodSource(OPTION_CASES + "hybridLimitsCases")
  default void hybridLimitsNumberOrObjectSetsEachReadLimit(HybridLimitsCase c) {
    assertHybridLimitsCase(c);
  }

  static Stream<Named<HybridLimitsCase>> hybridLimitsCases() {
    return Stream.of(
        hybridLimits("hybridLimits is 30 -> LIMIT 30 and 30", "30", 30, 30),
        hybridLimits("hybridLimits is 100, the upper bound", "100", 100, 100),
        hybridLimits("hybridLimits is 1, the lower bound", "1", 1, 1),
        hybridLimits("object $vector 80, $lexical 20", "{'$vector':80,'$lexical':20}", 80, 20),
        hybridLimits("object $vector 100, $lexical 1", "{'$vector':100,'$lexical':1}", 100, 1),
        hybridLimits("object $vector 1, $lexical 100", "{'$vector':1,'$lexical':100}", 1, 100),
        hybridLimits("hybridLimits is 2, below limit 10", "2", 2, 2),
        hybridLimits("object $vector 2, $lexical 2", "{'$vector':2,'$lexical':2}", 2, 2));
  }

  // An option of a JSON type the docs do not allow gives REQUEST_STRUCTURE_MISMATCH with the
  // parser's reason. Per the findAndRerank docs (Parameters and Properties of options).
  @ParameterizedTest(name = "{0}")
  @MethodSource(OPTION_CASES + "wrongTypeCases")
  default void optionWithWrongJsonTypeIsRejected(OptionError c) {
    assertRejected(c);
  }

  static Stream<Named<OptionError>> wrongTypeCases() {
    String structure = "Request is valid JSON but has a structural mismatch";
    String noCreator = "Cannot construct instance of";
    String notString = "Cannot deserialize value of type `java.lang.String`";
    String notBoolean = "only \"true\"/\"True\"/\"TRUE\" or \"false\"/\"False\"/\"FALSE\"";
    String hybridType = "hybridLimits must be an integer or an object, got ";
    String fields = "Expected fields: $lexical, $vector";
    return Stream.of(
        mismatch("options is a string", "'abc'", noCreator),
        mismatch("options is a number", "5", noCreator),
        mismatch("options is a boolean", "true", noCreator),
        mismatch("options is an empty string", "''", "Cannot coerce empty String"),
        mismatch("options is an array", "[{'limit': 5}]", "from Array value"),
        mismatch("limit is a non-numeric string", "{'limit': 'abc'}", "from String \"abc\""),
        mismatch("limit is a boolean", "{'limit': true}", structure),
        mismatch("limit is an array", "{'limit': [5]}", structure),
        mismatch("limit is an object", "{'limit': {}}", structure),
        mismatch("hybridLimits is a string", "{'hybridLimits': '50'}", hybridType + "STRING"),
        mismatch("hybridLimits is a boolean", "{'hybridLimits': true}", hybridType + "BOOLEAN"),
        mismatch("hybridLimits is an array", "{'hybridLimits': [10]}", hybridType + "ARRAY"),
        mismatch(
            "hybridLimits object lacks $vector",
            "{'hybridLimits': {'$lexical': 10}}",
            "hybridLimits has missing fields. " + fields + ". Missing fields: $vector"),
        mismatch("hybridLimits is an empty object", "{'hybridLimits': {}}", "has missing fields"),
        mismatch(
            "hybridLimits object has the extra key $vectorize",
            "{'hybridLimits': {'$vector': 10, '$lexical': 10, '$vectorize': 5}}",
            "contained unexpected fields. " + fields + ". Unexpected fields: $vectorize"),
        mismatch(
            "hybridLimits $vector is a string",
            limits("'10'", 10),
            fields + " to all be of type: MatchResult. Wrong type fields: $vector"),
        mismatch("hybridLimits $vector is null", limits(null, 10), "Wrong type fields: $vector"),
        mismatch("hybridLimits $lexical is an object", limits(10, "{}"), "type fields: $lexical"),
        mismatch("rerankQuery is an object", "{'rerankQuery': {'text': 'x'}}", notString),
        mismatch("rerankQuery is an array", "{'rerankQuery': ['x']}", notString),
        mismatch("rerankOn is an object", "{'rerankOn': {'a': 1}}", notString),
        mismatch("rerankOn is an array", "{'rerankOn': ['a']}", notString),
        mismatch("includeScores is yes", "{'includeScores': 'yes'}", notBoolean),
        mismatch("includeScores is 1.5", "{'includeScores': 1.5}", structure),
        mismatch("includeScores is an array", "{'includeScores': []}", structure),
        mismatch("includeScores is an object", "{'includeScores': {}}", structure),
        mismatch("includeSortVector is yes", "{'includeSortVector': 'yes'}", notBoolean),
        mismatch("includeSortVector is 1.5", "{'includeSortVector': 1.5}", structure),
        mismatch("includeSortVector is an array", "{'includeSortVector': []}", structure),
        mismatch("includeSortVector is an object", "{'includeSortVector': {}}", structure));
  }

  // Options that findAndRerank does not have give COMMAND_FIELD_UNKNOWN listing the known options.
  // Per the findAndRerank API brainstorm design doc (Reading, Options including limit), which lists
  // skip, pageState and includeSimilarity as find-only options.
  @ParameterizedTest(name = "{0}")
  @MethodSource(OPTION_CASES + "unknownOptionCases")
  default void unknownOptionNameIsRejected(OptionError c) {
    assertRejected(c);
  }

  static Stream<Named<OptionError>> unknownOptionCases() {
    return Stream.of(
        unknown("options has skip", "skip"),
        unknown("options has pageState", "pageState"),
        unknown("options has includeSimilarity", "includeSimilarity"),
        unknown("options has the Java name rerankServiceOverride", "rerankServiceOverride"),
        unknown("options has Limit with a capital L", "Limit"));
  }

  // limit below 1 and hybridLimits outside 1 to 100 give COMMAND_FIELD_VALUE_INVALID naming the
  // field and value ($lexical is checked even without a lexical sort); a rerankOn that is not a
  // valid path gives UNSUPPORTED_UPDATE_OPERATION_PATH. The docs do not specify these checks; the
  // test pins current behavior so that any change is visible in review.
  @ParameterizedTest(name = "{0}")
  @MethodSource(OPTION_CASES + "invalidValueCases")
  default void optionValueOutOfRangeOrInvalidPathIsRejected(OptionError c) {
    assertRejected(c);
  }

  static Stream<Named<OptionError>> invalidValueCases() {
    String limit = "Command field 'command.options.limit' value ";
    String vector = "Command field 'hybridLimits.$vector' value ";
    String lexical = "Command field 'hybridLimits.$lexical' value ";
    String range = " not valid: must be between 1 and 100 (inclusive)";
    ErrorCode<?> path = UpdateException.Code.UNSUPPORTED_UPDATE_OPERATION_PATH;
    String ampersand = "The ampersand character '&' at position 1 must be followed by either";
    return Stream.of(
        invalid("limit is 0", "{'limit': 0}", limit + "0 not valid: limit should be greater than"),
        invalid("limit is -3", "{'limit': -3}", limit + "-3 not valid"),
        invalid("hybridLimits is 0", "{'hybridLimits': 0}", vector + "0" + range),
        invalid("hybridLimits is -1", "{'hybridLimits': -1}", vector + "-1" + range),
        invalid("hybridLimits is 101", "{'hybridLimits': 101}", vector + "101" + range),
        invalid("hybridLimits $vector is 101", limits(101, 10), vector + "101" + range),
        invalid("hybridLimits $lexical is 0", limits(10, 0), lexical + "0" + range),
        invalid("$lexical is 101, the sort has no lexical part", limits(10, 101), lexical + "101"),
        invalid("$vector 0, $lexical 101 -> only $vector", limits(0, 101), vector + "0" + range),
        rejected("rerankOn is a..b", "{'rerankOn': 'a..b'}", path, "path ('a..b') is not valid"),
        rejected("rerankOn is a.", "{'rerankOn': 'a.'}", path, "Path cannot end with a dot"),
        rejected("rerankOn is a&b", "{'rerankOn': 'a&b'}", path, ampersand),
        rejected("rerankOn is a&", "{'rerankOn': 'a&'}", path, ampersand));
  }

  // limit 0.5 and hybridLimits 0.9 truncate to 0 and hybridLimits 3000000000 wraps to a negative
  // int, so they fail the range check; limit 3000000000 gives UNEXPECTED_SERVER_ERROR. The docs
  // give both options the type integer. The test pins current behavior; whether it is a bug is
  // still under discussion.
  @ParameterizedTest(name = "{0}")
  @MethodSource(OPTION_CASES + "truncatedToInvalidCases")
  default void limitNumbersTruncatedToInvalidValuesAreRejected(OptionError c) {
    assertRejected(c);
  }

  static Stream<Named<OptionError>> truncatedToInvalidCases() {
    return Stream.of(
        invalid("limit is 0.5 -> 0", "{'limit': 0.5}", "'command.options.limit' value 0 not valid"),
        invalid("hybridLimits is 0.9 -> 0", "{'hybridLimits': 0.9}", "$vector' value 0 not valid"),
        invalid(
            "hybridLimits is 3000000000 -> wraps to a negative int",
            "{'hybridLimits': 3000000000}",
            "'hybridLimits.$vector' value -1294967296 not valid"),
        rejected(
            "limit is 3000000000 -> UNEXPECTED_SERVER_ERROR",
            "{'limit': 3000000000}",
            ServerException.Code.UNEXPECTED_SERVER_ERROR,
            "Error Class: InputCoercionException"));
  }

  // A $vector sort without a usable rerankQuery (or with a null $vectorize) gives
  // MISSING_RERANK_QUERY_TEXT, and without a usable rerankOn MISSING_RERANK_ON. Per the
  // findAndRerank docs (Properties of options, rerankOn and rerankQuery), both needed for $vector.
  @ParameterizedTest(name = "{0}")
  @MethodSource(OPTION_CASES + "missingRerankQueryOrRerankOnCases")
  default void vectorSortWithoutRerankQueryOrRerankOnIsRejected(OptionError c) {
    assertRejected(c);
  }

  static Stream<Named<OptionError>> missingRerankQueryOrRerankOnCases() {
    String sort = "{'$hybrid': {'$vector': " + EXAMPLE_VECTOR + "}}";
    String nullText = "{'$hybrid': {'$vectorize': null, '$vector': [0.1, 0.16, 0.31, 0.22, 0.15]}}";
    String title = "{'rerankOn': 'title', 'rerankQuery': ";
    ErrorCode<?> query = RequestException.Code.MISSING_RERANK_QUERY_TEXT;
    ErrorCode<?> field = RequestException.Code.MISSING_RERANK_ON;
    return Stream.of(
        missing("rerankQuery missing", BYO, sort, "{'rerankOn': 'title'}", query),
        missing("rerankQuery is empty", BYO, sort, title + "''}", query),
        missing("rerankQuery is blank", BYO, sort, title + "' '}", query),
        missing("$vectorize null with $vector", MAIN, nullText, "{'rerankOn': 'title'}", query),
        missing("rerankOn missing", BYO, sort, "{'rerankQuery': 'q'}", field),
        missing("rerankOn is empty", BYO, sort, "{'rerankQuery': 'q', 'rerankOn': ''}", field),
        missing("rerankOn is blank", BYO, sort, "{'rerankQuery': 'q', 'rerankOn': ' '}", field),
        missing("rerankOn is null", BYO, sort, "{'rerankQuery': 'q', 'rerankOn': null}", field));
  }

  // Vectorize sort on m01 to m05: the query is the $vectorize text unless rerankQuery is set, and
  // status has sortVector (the query vector) or documentResponses only when its flag is true. Per
  // the findAndRerank docs (Properties of options): rerankQuery defaults to the query of the vector
  // search, and includeScores and includeSortVector default to false.
  @ParameterizedTest(name = "{0}")
  @MethodSource(OPTION_CASES + "documentedQueryAndFlagCases")
  default void rerankQueryAndFlagsWorkAsDocumented(DistinctCase c) {
    assertDistinctCase(c);
  }

  static Stream<Named<DistinctCase>> documentedQueryAndFlagCases() {
    String bothFalse = "{'includeScores': false, 'includeSortVector': false}";
    String bothNull = "{'includeScores': null, 'includeSortVector': null}";
    return Stream.of(
        query("options empty -> the $vectorize text, neither flag", "{}", OPTION_QUERY),
        query("rerankQuery is null -> the $vectorize text", "{'rerankQuery': null}", OPTION_QUERY),
        query("rerankQuery is set -> that text", "{'rerankQuery': 'Is it new'}", "Is it new"),
        flag("both flags false -> neither", bothFalse, false, false),
        flag("both flags null -> neither", bothNull, false, false),
        flag("includeSortVector true -> sortVector", "{'includeSortVector': true}", false, true));
  }

  // rerankQuery empty or blank falls back to the $vectorize text; any other text, even spaces or a
  // very long one, is sent exactly as written. The docs do not specify this; the test pins current
  // behavior so that any change is visible in review.
  @ParameterizedTest(name = "{0}")
  @MethodSource(OPTION_CASES + "rerankQueryEdgeCases")
  default void rerankQueryEdgeValuesAreSentAsWritten(DistinctCase c) {
    assertDistinctCase(c);
  }

  static Stream<Named<DistinctCase>> rerankQueryEdgeCases() {
    String longText = "long query ".repeat(500);
    String longOptions = "{'rerankQuery': '" + longText + "'}";
    return Stream.of(
        query("rerankQuery is empty -> the $vectorize text", "{'rerankQuery': ''}", OPTION_QUERY),
        query("rerankQuery is blank -> the $vectorize text", "{'rerankQuery': '  '}", OPTION_QUERY),
        query("rerankQuery has outer spaces -> kept", "{'rerankQuery': ' cheese '}", " cheese "),
        query("rerankQuery is a no-break space -> as is", "{'rerankQuery': '\u00a0'}", "\u00a0"),
        query("rerankQuery has 5500 characters -> as is", longOptions, longText));
  }

  // rerankQuery 123, true and 1.5 are sent as text; the flags turn on for "true" in any case and
  // non-zero integers. The docs (Properties of options) give rerankQuery the type string and the
  // flags boolean; main converts instead of rejecting. The test pins current behavior; whether it
  // is a bug is still under discussion.
  @ParameterizedTest(name = "{0}")
  @MethodSource(OPTION_CASES + "convertedScalarCases")
  default void rerankQueryAndFlagsConvertOtherScalarTypes(DistinctCase c) {
    assertDistinctCase(c);
  }

  static Stream<Named<DistinctCase>> convertedScalarCases() {
    return Stream.of(
        query("rerankQuery is 123 -> query 123", "{'rerankQuery': 123}", "123"),
        query("rerankQuery is true -> query true", "{'rerankQuery': true}", "true"),
        query("rerankQuery is 1.5 -> query 1.5", "{'rerankQuery': 1.5}", "1.5"),
        scoresFlag("includeScores is the string true -> on", "'true'", true),
        scoresFlag("includeScores is TRUE -> on", "'TRUE'", true),
        scoresFlag("includeScores is True -> on", "'True'", true),
        scoresFlag("includeScores is the string false -> off", "'false'", false),
        scoresFlag("includeScores is an empty string -> off", "''", false),
        scoresFlag("includeScores is 1 -> on", "1", true),
        scoresFlag("includeScores is 5 -> on", "5", true),
        scoresFlag("includeScores is 0 -> off", "0", false),
        sortVectorFlag("includeSortVector is the string true -> on", "'true'", true),
        sortVectorFlag("includeSortVector is TRUE -> on", "'TRUE'", true),
        sortVectorFlag("includeSortVector is the string false -> off", "'false'", false),
        sortVectorFlag("includeSortVector is an empty string -> off", "''", false),
        sortVectorFlag("includeSortVector is 1 -> on", "1", true),
        sortVectorFlag("includeSortVector is 0 -> off", "0", false));
  }

  // includeScores with rerankOn missing or blank: the stored $vectorize texts are the passages and
  // m13 (only a $vector, vector rank 1) is dropped but keeps its rank, so m02 has rank 2. The docs
  // (Properties of options, rerankOn) say the default is $lexical and do not cover dropped ranks.
  // The test pins current behavior; whether it is a bug is still under discussion.
  @ParameterizedTest(name = "{0}")
  @MethodSource(OPTION_CASES + "defaultRerankOnCases")
  default void rerankOnMissingOrBlankUsesStoredVectorizeText(String options) {
    String filter = json("{'grp': {'$in': ['distinct', 'vec-only']}}");
    String command = findAndRerank().filter(filter).sort(OPTION_SORT).options(json(options)).json();
    var response = postToFixture(MAIN, command);
    assertReranked(response, OPTION_QUERY, DISTINCT_RANKED, vectorizeOf(DISTINCT_RANKED));
    assertNoDollarFields(response);
    assertScores(response, distinctScores(1));
  }

  static Stream<Named<String>> defaultRerankOnCases() {
    return Stream.of(
        Named.of("rerankOn missing -> $vectorize", "{'includeScores': true}"),
        Named.of("rerankOn is empty -> $vectorize", "{'includeScores': true, 'rerankOn': ''}"),
        Named.of("rerankOn is blank -> $vectorize", "{'includeScores': true, 'rerankOn': '  '}"));
  }

  // rerankOn title (8 of 13 documents have one), $vectorize (12) and _id (all 13): those values are
  // the passages, documents without the field are dropped and the top 10 return. Per the
  // findAndRerank docs (Properties of options, rerankOn): documents without the field are excluded.
  @ParameterizedTest(name = "{0}")
  @MethodSource(OPTION_CASES + "rerankOnFieldCases")
  default void rerankOnNamesTheFieldUsedAsPassage(PassageCase c) {
    assertPassageCase(c);
  }

  static Stream<Named<PassageCase>> rerankOnFieldCases() {
    List<String> byTitle = ids("m06 m01 m08 m03 m05 m02 m07 m04");
    List<String> top10 = ALL_RANKED.subList(0, 10);
    List<String> all12 = vectorizeOf(ALL_RANKED);
    List<String> byId = ids("m06 m10 m02 m08 m04 m13 m12 m01 m07 m05");
    List<String> all13 = IntStream.rangeClosed(1, 13).mapToObj(i -> "m%02d".formatted(i)).toList();
    return Stream.of(
        passages("rerankOn is title -> 8 documents", "title", byTitle, mainField("title", byTitle)),
        passages("rerankOn is $vectorize -> 12 documents", "$vectorize", top10, all12),
        passages("rerankOn is _id -> 13 documents", "_id", byId, all13));
  }

  // rerankOn $lexical on the plain documents (HCD only): the stored $lexical texts are the
  // passages, m08 without $lexical is dropped, and the result has no $lexical. Per the
  // findAndRerank docs (Properties of options, rerankOn; Parameters, projection).
  @Test
  @DisabledIfSystemProperty(named = TEST_PROP_LEXICAL_DISABLED, matches = "true")
  default void rerankOnLexicalUsesStoredLexicalText() {
    List<String> passages =
        List.of(
            "maps islands sailors", // m07
            "tomatoes balcony garden", // m12
            "Lanterns glow over the quiet harbor", // m06, written with a $hybrid string
            "bicycle chain repair", // m10
            "marathon runner training", // m09
            "night sky photography"); // m11
    List<String> ranked = ids("m07 m12 m06 m10 m09 m11");
    assertPassageCase(new PassageCase("{\"grp\": \"plain\"}", "$lexical", ranked, passages));
  }

  // rerankOn $lexical on a collection without lexical, and $vectorize on one without vectorize
  // (with a $vector sort): an empty result without an error and without a reranker call. Per the
  // findAndRerank docs (Properties of options, rerankOn): documents without the field are excluded.
  @Test
  default void rerankOnFieldMissingFromEveryDocumentGivesEmptyResult() {
    String lexical = findAndRerank().sort(OPTION_SORT).option("rerankOn", "$lexical").json();
    assertReranked(postToFixture(NOLEX, lexical), OPTION_QUERY, List.of(), List.of());
    String sort = "{\"$hybrid\": {\"$vector\": " + EXAMPLE_VECTOR + "}}";
    String options = json("{'rerankQuery': 'q', 'rerankOn': '$vectorize'}");
    String vectorize = findAndRerank().sort(sort).options(options).json();
    assertReranked(postToFixture(BYO, vectorize), "q", List.of(), List.of());
  }

  // rerankOn path forms: a dotted path, an array index and an escaped dot select passages; paths
  // that no document has give an empty result and no reranker call. The docs do not specify this;
  // the test pins current behavior so that any change is visible in review.
  @ParameterizedTest(name = "{0}")
  @MethodSource(OPTION_CASES + "rerankOnPathCases")
  default void rerankOnPathFormsSelectPassages(PassageCase c) {
    assertPassageCase(c);
  }

  static Stream<Named<PassageCase>> rerankOnPathCases() {
    List<String> none = List.of();
    return Stream.of(
        passages("rerankOn is a.b -> nested field of m03", "a.b", ids("m03"), List.of("x")),
        passages("rerankOn is tags.0 -> first tag of m02", "tags.0", ids("m02"), List.of("x")),
        passages("rerankOn is a&.b -> field a.b of m05", "a&.b", ids("m05"), List.of("z")),
        passages("rerankOn is title with spaces around it -> none", " title ", none, none),
        passages("rerankOn is a 1000 character name -> none", "x".repeat(1000), none, none),
        passages("rerankOn is $similarity, no includeScores -> none", "$similarity", none, none));
  }

  // The docs (Properties of options, rerankOn) give rerankOn the type string and exclude missing,
  // null and non-string values; main takes a number or boolean rerankOn as a path no document has,
  // reranks number and boolean fields on their text, drops null or blank text and keeps other text
  // untrimmed. The test pins current behavior; whether it is a bug is still under discussion.
  @ParameterizedTest(name = "{0}")
  @MethodSource(OPTION_CASES + "scalarPassageCases")
  default void rerankOnUsesScalarTextAndDropsNullOrBlankText(PassageCase c) {
    assertPassageCase(c);
  }

  static Stream<Named<PassageCase>> scalarPassageCases() {
    List<String> none = List.of();
    List<String> edge = List.of("  Hello  ", "\u00a0");
    return Stream.of(
        passages("rerankOn is the number 123 -> none", 123, none, none),
        passages("rerankOn is the boolean true -> none", true, none, none),
        passages("rerankOn is n, the integer 42 -> passage 42", "n", ids("m06"), List.of("42")),
        passages("rerankOn is d, the decimal 1.50 -> passage 1.5", "d", ids("m07"), List.of("1.5")),
        passages("rerankOn is flag, true -> passage true", "flag", ids("m08"), List.of("true")),
        passages("rerankOn is edge -> null and blank dropped", "edge", ids("m01 m12"), edge));
  }

  // The docs (Properties of options, rerankOn) exclude documents whose field is not a string;
  // rerankOn naming an object (meta), an array (tags) or $vector fails with UNEXPECTED_SERVER_ERROR
  // after the reads. The test pins current behavior; whether it is a bug is still under discussion.
  @ParameterizedTest(name = "{0}")
  @MethodSource(OPTION_CASES + "objectPassageCases")
  default void rerankOnObjectArrayOrVectorFailsTheRequest(OptionError c) {
    assertRuntimeError(postToFixture(c.fixture(), c.command()), c.code(), 0, c.snippets());
  }

  static Stream<Named<OptionError>> objectPassageCases() {
    return Stream.of(
        serverError("rerankOn is meta, an object in m01", "meta"),
        serverError("rerankOn is tags, an array in m02", "tags"),
        serverError("rerankOn is $vector, an array in every document", "$vector"));
  }

  // rerankOn $similarity with includeScores and reads limited to 1: "a" (vector read) gets its
  // similarity "1.0" as passage; "b" (BM25 read, HCD only) has none and is dropped. The docs do
  // not specify this; the test pins current behavior so that any change is visible in review.
  @Test
  default void rerankOnSimilarityUsesTheVectorScoreText() {
    String hashScores = FakeRerankerModes.mode(FakeRerankerModes.Key.HASH_SCORES);
    FakeRerankerClient.stubMode(hashScores);
    String lexical = lexicalAvailable() ? ", \"$lexical\": \"" + IDS_TERM + "\"" : "";
    String command =
        findAndRerank()
            .filter("{\"pairs\": \"p-str\"}")
            .sort("{\"$hybrid\": {\"$vector\": " + IDS_VECTOR + lexical + "}}")
            .options(json("{'includeScores': true, 'rerankOn': '$similarity'}"))
            .options(json("{'hybridLimits': 1, 'rerankQuery': '" + hashScores + "'}"))
            .json();
    var response = postToFixture(IDS, command);
    assertReranked(response, hashScores, List.of("a"), List.of("1.0"));
    assertScores(response, scores(FakeRerankerScores.hashScore("1.0"), 1.0f, 1, null, rrf(1)));
  }

  // includeScores true: status.documentResponses has one {"scores": {...}} per document with
  // exactly $rerank, $vector, $vectorRank, $bm25Rank (null here) and $rrf. The docs (Properties of
  // options, includeScores) show {"$vector": 0.81, "$rerank": 0.12} without the "scores" wrapper.
  // The test pins current behavior; whether it is a bug is still under discussion.
  @Test
  default void includeScoresTrueReturnsFiveScoresPerDocument() {
    assertDistinctCase(new DistinctCase("{'includeScores': true}", OPTION_QUERY, true, false));
  }

  record LimitCase(String options, int returned) {}

  record HybridLimitsCase(String hybridLimits, int vectorLimit, int lexicalLimit) {}

  record OptionError(Fixture fixture, String command, ErrorCode<?> code, String... snippets) {}

  record DistinctCase(String options, String query, boolean scores, boolean sortVector) {}

  record PassageCase(String filter, Object rerankOn, List<String> ids, List<String> passages) {}

  private static Named<LimitCase> limit(String description, String options, int returned) {
    return Named.of(description, new LimitCase(options, returned));
  }

  private static Named<HybridLimitsCase> hybridLimits(
      String description, String hybridLimits, int vectorLimit, int lexicalLimit) {
    return Named.of(description, new HybridLimitsCase(hybridLimits, vectorLimit, lexicalLimit));
  }

  private static Named<DistinctCase> query(String description, String options, String query) {
    return Named.of(description, new DistinctCase(options, query, false, false));
  }

  private static Named<DistinctCase> flag(
      String description, String options, boolean scores, boolean sortVector) {
    return Named.of(description, new DistinctCase(options, OPTION_QUERY, scores, sortVector));
  }

  private static Named<DistinctCase> scoresFlag(String description, String value, boolean on) {
    return flag(description, "{'includeScores': " + value + "}", on, false);
  }

  private static Named<DistinctCase> sortVectorFlag(String description, String value, boolean on) {
    return flag(description, "{'includeSortVector': " + value + "}", false, on);
  }

  private static Named<PassageCase> passages(
      String description, Object rerankOn, List<String> ids, List<String> passages) {
    return Named.of(description, new PassageCase(null, rerankOn, ids, passages));
  }

  /** {@link #OPTION_SORT} on the main collection with these options must fail. */
  private static Named<OptionError> rejected(
      String description, String options, ErrorCode<?> code, String... snippets) {
    return Named.of(description, new OptionError(MAIN, withOptions(options), code, snippets));
  }

  private static Named<OptionError> mismatch(String description, String options, String snippet) {
    return rejected(
        description, options, RequestException.Code.REQUEST_STRUCTURE_MISMATCH, snippet);
  }

  private static Named<OptionError> invalid(String description, String options, String snippet) {
    return rejected(
        description, options, RequestException.Code.COMMAND_FIELD_VALUE_INVALID, snippet);
  }

  private static Named<OptionError> unknown(String description, String name) {
    ErrorCode<?> code = RequestException.Code.COMMAND_FIELD_UNKNOWN;
    String known =
        "' not recognized: not one of known fields ('hybridLimits', 'includeScores',"
            + " 'includeSortVector', 'limit', 'rerank', 'rerankOn', 'rerankQuery')";
    return rejected(description, "{'" + name + "': 1}", code, "Command field '" + name + known);
  }

  private static Named<OptionError> serverError(String description, String rerankOn) {
    return rejected(
        description,
        "{'rerankOn': '" + rerankOn + "'}",
        ServerException.Code.UNEXPECTED_SERVER_ERROR,
        "Error Class: IllegalArgumentException",
        "Passage field " + rerankOn + " is present but not null or a valueNode");
  }

  /** A {@code $hybrid} object sort without text; the expected error is chosen by its code. */
  private static Named<OptionError> missing(
      String description, Fixture fixture, String sort, String options, ErrorCode<?> code) {
    String command = findAndRerank().sort(json(sort)).options(json(options)).json();
    String snippet =
        code == RequestException.Code.MISSING_RERANK_ON
            ? "does not specify which document field to rerank on"
            : "is missing the text to use as the query with the reranking model";
    return Named.of(description, new OptionError(fixture, command, code, snippet));
  }

  /** findAndRerank with {@link #OPTION_SORT} and these single-quoted options, none if null. */
  private static String withOptions(String options) {
    String optionsPart = options == null ? "" : ", \"options\": " + json(options);
    return "{\"findAndRerank\": {\"sort\": " + OPTION_SORT + optionsPart + "}}";
  }

  /** Single-quoted options with hybridLimits as an object; the values are JSON text. */
  private static String limits(Object vector, Object lexical) {
    return "{'hybridLimits': {'$vector': " + vector + ", '$lexical': " + lexical + "}}";
  }

  private static String json(String singleQuoted) {
    return singleQuoted.replace('\'', '"');
  }

  private static List<String> ids(String spaceSeparated) {
    return List.of(spaceSeparated.split(" "));
  }

  private static List<String> vectorizeOf(List<String> ids) {
    return mainField("$vectorize", ids);
  }

  /** The text of {@code field} in these main documents, from the fixture as written on DSE. */
  private static List<String> mainField(String field, List<String> ids) {
    Map<String, String> byId = new HashMap<>();
    for (String document : MAIN.documents(false)) {
      JsonNode node = FindAndRerankRequests.parse(document);
      if (node.has(field)) {
        byId.put(node.get("_id").asText(), node.get(field).asText());
      }
    }
    return ids.stream().map(byId::get).toList();
  }

  /** includeScores of m05, m03, m02, m04, m01 for OPTION_SORT; vector ranks plus rankShift. */
  private static ExpectedScores[] distinctScores(int rankShift) {
    float[] rerank = {3.25f, 2.125f, 1.5f, 0.5f, -0.75f};
    float[] vector = {0.93070626f, 0.8224976f, 0.9787127f, 0.9768599f, 0.7665279f};
    int[] rank = IntStream.of(3, 4, 1, 2, 5).map(r -> r + rankShift).toArray();
    return IntStream.range(0, 5)
        .mapToObj(i -> scores(rerank[i], vector[i], rank[i], null, rrf(rank[i])))
        .toArray(ExpectedScores[]::new);
  }

  /** The distinct reads in status.trace, sorted, as "ANN n" (vector read) or "BM25 n" (LIMIT n). */
  private static List<String> readLimits(ValidatableResponse response) {
    return Pattern.compile("(ANN|BM25) OF \\? LIMIT (\\d+)")
        .matcher(response.extract().asString())
        .results()
        .map(match -> match.group(1) + " " + match.group(2))
        .distinct()
        .sorted()
        .toList();
  }

  /** Exactly these ids, no warning, and these passages and query sent to the default model. */
  private static void assertReranked(
      ValidatableResponse response, String query, List<String> ids, List<String> passages) {
    assertIds(response, ids.toArray());
    response.body("status.warnings", nullValue());
    if (passages.isEmpty()) {
      assertRerankerNotCalled();
    } else {
      assertRerankerCalls(defaultModelCalls(query, passages.toArray(String[]::new)));
    }
  }

  private static void assertStatusLacks(ValidatableResponse response, String... keys) {
    Map<String, Object> status = response.extract().jsonPath().getMap("status");
    assertThat(status == null ? Map.<String, Object>of() : status).doesNotContainKeys(keys);
  }

  private void assertRejected(OptionError c) {
    assertApiError(postToFixture(c.fixture(), c.command()), c.code(), c.snippets());
  }

  /** The default headers plus full request tracing, so the response shows the read CQL. */
  private Map<String, Object> traceHeaders() {
    return FindAndRerankRequests.headers(ApiFeature.REQUEST_TRACING_FULL.httpHeaderName(), "true");
  }

  private void assertLimitCase(LimitCase c) {
    var response = postToFixture(MAIN, withOptions(c.options()), traceHeaders());
    assertReranked(
        response, OPTION_QUERY, ALL_RANKED.subList(0, c.returned()), vectorizeOf(ALL_RANKED));
    assertThat(readLimits(response)).as("read limits").isEqualTo(List.of("ANN 50"));
    assertStatusLacks(response, "documentResponses", "sortVector");
  }

  /**
   * Sends {@link #OPTION_HYBRID_TEXT} to m01 to m05 with limit 10; expects the first vectorLimit of
   * the vector order and, on HCD, the first lexicalLimit of the BM25 order, in rerank order.
   */
  private void assertHybridLimitsCase(HybridLimitsCase c) {
    String extra = c.hybridLimits() == null ? "" : ", 'hybridLimits': " + c.hybridLimits();
    var request = findAndRerank().filter(OPTION_DISTINCT).hybrid(OPTION_HYBRID_TEXT);
    String command = request.options(json("{'limit': 10" + extra + "}")).json();
    var response = postToFixture(MAIN, command, traceHeaders());
    Set<String> found = new HashSet<>();
    ids("m04 m05 m02 m01 m03").stream().limit(c.vectorLimit()).forEach(found::add);
    List<String> limits = new ArrayList<>(List.of("ANN " + c.vectorLimit()));
    if (lexicalAvailable()) {
      ids("m01 m02 m04 m03 m05").stream().limit(c.lexicalLimit()).forEach(found::add);
      limits.add("BM25 " + c.lexicalLimit());
    }
    List<String> expected = DISTINCT_RANKED.stream().filter(found::contains).toList();
    assertReranked(response, OPTION_HYBRID_TEXT, expected, vectorizeOf(expected));
    assertThat(readLimits(response)).as("read limits").isEqualTo(limits);
  }

  /** Sends {@link #OPTION_SORT} to m01 to m05; checks the result, query and status flags. */
  private void assertDistinctCase(DistinctCase c) {
    var request = findAndRerank().filter(OPTION_DISTINCT).sort(OPTION_SORT);
    var response = postToFixture(MAIN, request.options(json(c.options())).json());
    assertReranked(response, c.query(), DISTINCT_RANKED, vectorizeOf(DISTINCT_RANKED));
    if (c.scores()) {
      assertScores(response, distinctScores(0));
    } else {
      assertStatusLacks(response, "documentResponses");
    }
    if (c.sortVector()) {
      assertSortVector(response, 0.1, 0.16, 0.31, 0.22, 0.15);
    } else {
      assertStatusLacks(response, "sortVector");
    }
  }

  private void assertPassageCase(PassageCase c) {
    var request = c.filter() == null ? findAndRerank() : findAndRerank().filter(c.filter());
    var response =
        postToFixture(MAIN, request.sort(OPTION_SORT).option("rerankOn", c.rerankOn()).json());
    assertReranked(response, OPTION_QUERY, c.ids(), c.passages());
    assertNoDollarFields(response);
  }
}
