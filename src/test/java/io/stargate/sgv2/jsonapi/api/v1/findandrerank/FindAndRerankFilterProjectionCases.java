package io.stargate.sgv2.jsonapi.api.v1.findandrerank;

import static io.stargate.sgv2.jsonapi.api.v1.AbstractKeyspaceIntegrationTestBase.TEST_PROP_LEXICAL_DISABLED;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankAssertions.*;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.*;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankRequests.findAndRerank;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankRequests.parse;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.RerankerAssertions.*;
import static io.stargate.sgv2.jsonapi.exception.FilterException.Code.*;
import static io.stargate.sgv2.jsonapi.exception.ProjectionException.Code.*;
import static io.stargate.sgv2.jsonapi.exception.SchemaException.Code.LEXICAL_NOT_ENABLED_FOR_COLLECTION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import io.restassured.response.ValidatableResponse;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * findAndRerank tests about {@code filter} and {@code projection}: which documents a filter lets
 * into the reads, which fields a projection keeps, and the errors for invalid ones.
 *
 * <p>For reviewers: success tests assert the passages the reranker received, taken from the stored
 * documents before any projection, because they show which documents the reads found. An unparsable
 * filter is parsed again for metrics after the command and its error replaces the result, so it
 * gives a single error even when both reads fail. Methods ending in {@code OnHcd} need lexical.
 * Single quotes in the case JSON become double quotes.
 */
public interface FindAndRerankFilterProjectionCases extends FindAndRerankTestContext {

  String FILTER_PROJECTION_FACTORIES =
      "io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFilterProjectionCases#";

  // The query of the official examples; twelve frr_main documents have a passage for it.
  String TREE_QUERY = "A tree in the woods";
  // A test sentence with its own vector, so that the vector ranks are fixed.
  String CHATGPT_QUERY = "ChatGPT upgraded";
  // The ten frr_main documents with the highest rerank scores, best first.
  List<Object> MAIN_TOP_TEN =
      List.of("m08", "m05", "m11", "m03", "m06", "m02", "m12", "m04", "m09", "m01");
  // Fields of m05 and m02 under the default projection; OWN is m05's "a.b" and m02's "tags".
  String REGULAR = "_id grp title is_checked_out number_of_pages OWN";
  // m05 and m02 exactly as the default projection returns them.
  String M05_M02_DEFAULT =
      """
      [{'_id': 'm05', 'grp': 'distinct', 'title': 'Fresh data', 'is_checked_out': false,
        'number_of_pages': 120, 'a.b': 'z'},
       {'_id': 'm02', 'grp': 'distinct', 'title': 'Talking sneakers', 'is_checked_out': false,
        'number_of_pages': 150, 'tags': ['x', 'y']}]""";

  // The official example query on frr_main with no filter or "filter": null reranks all twelve
  // $vectorize passages and returns the best ten. Per the findAndRerank docs (Parameters, filter),
  // the default is no filter: any document may match.
  @ParameterizedTest(name = "{0}")
  @MethodSource(FILTER_PROJECTION_FACTORIES + "missingOrNullFilterCases")
  default void filterMissingOrNullReadsWholeCollection(String request) {
    assertIds(postToFixture(MAIN, request), MAIN_TOP_TEN.toArray());
    assertRerankerCalls(allTwelveReranked());
  }

  static Stream<Arguments> missingOrNullFilterCases() {
    return Stream.of(
        Arguments.of(Named.of("no filter -> whole collection", treeQuery(null, null))),
        Arguments.of(Named.of("filter null -> whole collection", treeQuery("null", null))));
  }

  // Same request with "filter": {} (whole collection) and an _id $in filter (only those ids; read
  // ranks follow completion order, so they are not asserted). The docs do not specify these forms;
  // the test pins current behavior so that any change is visible in review.
  @ParameterizedTest(name = "{0}")
  @MethodSource(FILTER_PROJECTION_FACTORIES + "emptyOrIdInFilterCases")
  default void filterEmptyObjectOrIdInSelectsCandidates(
      Fixture fixture, String request, List<Object> ids, ExpectedRerankCalls expected) {
    assertIds(postToFixture(fixture, request), ids.toArray());
    assertRerankerCalls(expected);
  }

  static Stream<Arguments> emptyOrIdInFilterCases() {
    return Stream.of(
        filter("filter {} -> whole collection", "{}").returns(MAIN_TOP_TEN, allTwelveReranked()),
        filter("_id $in three ids -> only those three", "{'_id': {'$in': ['m01', 'm03', 'm05']}}")
            .reranksAndReturns("m05", "m03", "m01"));
  }

  // On HCD, where both reads run: a regular filter on frr_byo drops b5 (BM25 only) and b8 (vector
  // only); $lexical $match filters on frr_main. Per the findAndRerank docs (Parameters, filter)
  // only matching documents take part; per the BM25 design doc $match may repeat in $or.
  @ParameterizedTest(name = "{0}")
  @MethodSource(FILTER_PROJECTION_FACTORIES + "filtersOnBothReadsCases")
  @DisabledIfSystemProperty(named = TEST_PROP_LEXICAL_DISABLED, matches = "true")
  default void filterRestrictsBothReadsOnHcd(
      Fixture fixture, String request, List<Object> ids, ExpectedRerankCalls expected) {
    assertIds(postToFixture(fixture, request), ids.toArray());
    assertRerankerCalls(expected);
  }

  static Stream<Arguments> filtersOnBothReadsCases() {
    String byo =
        findAndRerank()
            .filter(json("{'grp': 'order'}"))
            .sort(json("{'$hybrid': {'$vector': " + EXAMPLE_VECTOR + ", '$lexical': 'grassy'}}"))
            .options(json("{'rerankOn': '$lexical', 'rerankQuery': 'grassy'}"))
            .json();
    var orders = defaultModelCalls("grassy", passages(BYO, "$lexical", "o1", "o2", "o3", "o4"));
    String harborOrIslands =
        "{'$or': [{'$lexical': {'$match': 'harbor'}}, {'$lexical': {'$match': 'islands'}}]}";
    return Stream.of(
        new FilterCase("frr_byo grp order -> o1 to o4 only", BYO, byo)
            .returns(List.of("o4", "o3", "o2", "o1"), orders),
        filter("$lexical $match grassy -> m01 to m05", "{'$lexical': {'$match': 'grassy'}}")
            .reranksAndReturns("m05", "m03", "m02", "m04", "m01"),
        filter("$or of $match harbor and $match islands -> m06 and m07", harborOrIslands)
            .reranksAndReturns("m06", "m07"));
  }

  // Filters and projects frr_main on an escaped field name; expects only the document that has the
  // field, as just _id and that field: "a&.b" names m05's field "a.b" (m03 and m04 have a nested
  // a.b), "a&&b" the field "a&b" of a temporary document. Per the findAndRerank docs (Parameters,
  // filter and projection), "&" escapes "." and "&" in field names.
  @ParameterizedTest(name = "{0}")
  @MethodSource(FILTER_PROJECTION_FACTORIES + "escapedFieldNameCases")
  default void escapedFieldNameMatchesInFilterAndProjection(
      String request, String tmpDocument, Map<String, ?> document, ExpectedRerankCalls calls) {
    String collection = ensure(MAIN);
    ValidatableResponse response;
    try {
      if (tmpDocument != null) {
        String insert = json("{'insertOne': {'document': " + tmpDocument + "}}");
        postToCollection(collection, insert).statusCode(200).body("errors", is(nullValue()));
      }
      response = postToCollection(collection, request);
    } finally {
      if (tmpDocument != null) {
        postToCollection(collection, json("{'deleteMany': {'filter': {'grp': 'tmp'}}}"))
            .statusCode(200);
      }
    }
    assertIds(response, document.get("_id")).body("data.documents[0]", is(document));
    assertRerankerCalls(calls);
  }

  static Stream<Arguments> escapedFieldNameCases() {
    String dotted = treeQuery("{'a&.b': {'$exists': true}}", "{'a&.b': 1}");
    String ampersand = treeQuery("{'a&&b': 'x'}", "{'a&&b': 1}");
    String tmp = "{'_id': 't-amp', 'grp': 'tmp', 'a&b': 'x', '$vectorize': 'Updating new data'}";
    return Stream.of(
        Arguments.of(
            Named.of("a&.b -> m05, field a.b", dotted),
            null,
            Map.of("_id", "m05", "a.b", "z"),
            treeReranked("m05")),
        Arguments.of(
            Named.of("a&&b -> temporary document, field a&b", ampersand),
            tmp,
            Map.of("_id", "t-amp", "a&b", "x"),
            defaultModelCalls(TREE_QUERY, "Updating new data")));
  }

  // A filter that matches nothing gives an empty page, the sort vector if includeSortVector asks
  // for it, and no reranker call. Per the findAndRerank docs (Result), a search that finds nothing
  // returns no documents; they do not say whether the reranker is called, and the test pins that.
  @Test
  default void filterMatchingNothingReturnsEmptyPageWithoutReranking() {
    assertEmptyPageWithoutReranking("{'grp': 'no-such-group'}");
  }

  // Same with {"_id": {"$in": []}}, which skips the database queries altogether. The docs do not
  // specify this; the test pins current behavior so that any change is visible in review.
  @Test
  default void emptyIdInFilterReturnsEmptyPageWithoutReranking() {
    assertEmptyPageWithoutReranking("{'_id': {'$in': []}}");
  }

  // Sends filters and projection paths that the docs rule out; each fails with the listed error.
  // Per the findAndRerank docs (Parameters, filter and projection), a filter is an object in the
  // Data API filter syntax on indexed fields, and "&" only escapes "." and "&"; per the BM25 design
  // doc, $match is only for $lexical.
  @ParameterizedTest(name = "{0}")
  @MethodSource(FILTER_PROJECTION_FACTORIES + "documentedInvalidFilterCases")
  default void documentedInvalidFilterOrPathIsRejected(
      Fixture fixture, String request, ErrorCode<?> code, String snippet) {
    assertApiError(postToFixture(fixture, request), code, snippet);
  }

  static Stream<Arguments> documentedInvalidFilterCases() {
    String unindexed =
        findAndRerank()
            .filter(json("{'author': 'Ann'}"))
            .sort(json("{'$hybrid': {'$vector': " + IDS_VECTOR + "}}"))
            .options(json("{'rerankQuery': 'q', 'rerankOn': 'title'}"))
            .json();
    String lexicalFilter = treeQuery("{'$lexical': {'$match': 'cheese'}}", null);
    return Stream.of(
        filter("filter is a string", "'abc'")
            .fails(FILTER_UNSUPPORTED_DATA_TYPE, "command had String"),
        filter("filter is an array", "[]").fails(FILTER_UNSUPPORTED_DATA_TYPE, "command had Array"),
        filter("unknown operator $foo", "{'a': {'$foo': 1}}")
            .fails(FILTER_UNSUPPORTED_OPERATOR, "'$foo'"),
        filter("$match on a regular field", "{'title': {'$match': 'cheese'}}")
            .fails(FILTER_INVALID_EXPRESSION, "with the '$lexical' field, not 'title'"),
        new FilterCase("$lexical filter on a collection without lexical", NOLEX, lexicalFilter)
            .fails(LEXICAL_NOT_ENABLED_FOR_COLLECTION, "The collection without a lexical index"),
        new FilterCase("filter on a field left out of indexing", IDS, unindexed)
            .fails(FILTER_PATH_UNINDEXED, "Collection path 'author' is not indexed"),
        projection("projection a&b", "{'a&b': 1}")
            .fails(UNSUPPORTED_PROJECTION_PARAM, "('a&b') is not a valid"),
        filter("filter a&b", "{'a&b': 'x'}")
            .fails(FILTER_INVALID_EXPRESSION, "path ('a&b') is not valid"));
  }

  // On HCD, $lexical filters with an operator other than $match, or with $match on a number, fail
  // with FILTER_INVALID_EXPRESSION. Per the BM25 design doc, $lexical only takes $match, and $match
  // takes a string.
  @ParameterizedTest(name = "{0}")
  @MethodSource(FILTER_PROJECTION_FACTORIES + "invalidLexicalFilterCases")
  @DisabledIfSystemProperty(named = TEST_PROP_LEXICAL_DISABLED, matches = "true")
  default void invalidLexicalFilterFailsWhenReadsRunOnHcd(
      Fixture fixture, String request, ErrorCode<?> code, String snippet) {
    assertApiError(postToFixture(fixture, request), code, snippet);
  }

  static Stream<Arguments> invalidLexicalFilterCases() {
    return Stream.of(
        filter("$lexical with $eq", "{'$lexical': {'$eq': 'cheese'}}")
            .fails(FILTER_INVALID_EXPRESSION, "operator '$eq': only '$match' is supported"),
        filter("$match with a number", "{'$lexical': {'$match': 1}}")
            .fails(FILTER_INVALID_EXPRESSION, "must have `String` value, was `Number`"));
  }

  // On HCD, {"$lexical": "cheese"} and {"$lexical": {"$exists": false}}. The BM25 design doc makes
  // $match the default operator for $lexical and uses $exists in its migration script; main reads
  // the shorthand as $eq and rejects both. The test pins current behavior; whether it is a bug is
  // still under discussion.
  @ParameterizedTest(name = "{0}")
  @MethodSource(FILTER_PROJECTION_FACTORIES + "lexicalShorthandAndExistsCases")
  @DisabledIfSystemProperty(named = TEST_PROP_LEXICAL_DISABLED, matches = "true")
  default void lexicalFilterShorthandAndExistsFailOnHcd(
      Fixture fixture, String request, ErrorCode<?> code, String snippet) {
    assertApiError(postToFixture(fixture, request), code, snippet);
  }

  static Stream<Arguments> lexicalShorthandAndExistsCases() {
    return Stream.of(
        filter("$lexical shorthand string", "{'$lexical': 'cheese'}")
            .fails(FILTER_INVALID_EXPRESSION, "operator '$eq': only '$match' is supported"),
        filter("$lexical with $exists false", "{'$lexical': {'$exists': false}}")
            .fails(FILTER_INVALID_EXPRESSION, "operator '$exists': only '$match' is supported"));
  }

  // Reranks frr_main's m05 and m02 with includeScores and each projection. Expects exactly the
  // listed fields, m05 first, all scores, and never $similarity. Per the findAndRerank docs
  // (Parameters, projection), $ fields are left out unless included, _id is in unless excluded.
  @ParameterizedTest(name = "{0}")
  @MethodSource(FILTER_PROJECTION_FACTORIES + "documentedProjectionCases")
  default void documentedProjectionsShapeReturnedDocuments(String request, String fields) {
    // DSE stores no $lexical, so there it cannot be returned.
    String expected = lexicalAvailable() ? fields : fields.replace(" $lexical", "");
    var response = postToFixture(MAIN, request);
    assertFields(response, expected.replace("OWN", "a.b"), expected.replace("OWN", "tags"));
    var body = response.extract().jsonPath();
    if (fields.contains("_id")) {
      assertThat(body.getList("data.documents._id")).containsExactly("m05", "m02");
    } else {
      assertThat(body.getList("data.documents.title"))
          .containsExactly("Fresh data", "Talking sneakers");
    }
    var m05 = scores(3.25f, 0.93070626f, 2, null, 1f / 62);
    assertScores(response, m05, scores(1.5f, 0.9787127f, 1, null, 1f / 61));
    assertRerankerCalls(twoDocsReranked());
  }

  static Stream<Arguments> documentedProjectionCases() {
    return Stream.of(
        shaped("no projection -> _id and regular fields", null, REGULAR),
        shaped("{} -> same as no projection", "{}", REGULAR),
        shaped("include title -> _id and title", "{'title': 1}", "_id title"),
        shaped("include title, exclude _id -> title", "{'title': 1, '_id': 0}", "title"),
        shaped("exclude title -> all but title", "{'title': 0}", REGULAR.replace(" title", "")),
        shaped("exclude _id -> all but _id", "{'_id': 0}", REGULAR.replace("_id ", "")),
        shaped("include $vector -> adds it", "{'$vector': 1}", REGULAR + " $vector"),
        shaped("include title and $vector", "{'title': 1, '$vector': 1}", "_id title $vector"),
        shaped("include $vectorize -> adds it", "{'$vectorize': 1}", REGULAR + " $vectorize"),
        shaped("include $lexical -> adds it", "{'$lexical': 1}", REGULAR + " $lexical"),
        shaped("include * -> all stored", "{'*': 1}", REGULAR + " $vector $vectorize $lexical"));
  }

  // Same two documents with {"_id": 1}, {"*": 0} and a $slice: _id alone does not make an
  // inclusion projection, {"*": 0} leaves empty documents, $slice keeps the first tag. The docs do
  // not specify these; the test pins current behavior so that any change is visible in review.
  @ParameterizedTest(name = "{0}")
  @MethodSource(FILTER_PROJECTION_FACTORIES + "undocumentedProjectionCases")
  default void undocumentedProjectionsShapeReturnedDocuments(String request, String documents) {
    var response = postToFixture(MAIN, request).statusCode(200).body("errors", is(nullValue()));
    assertThat(parse(response.extract().asString()).at("/data/documents"))
        .isEqualTo(parse(json(documents)));
    assertRerankerCalls(twoDocsReranked());
  }

  static Stream<Arguments> undocumentedProjectionCases() {
    String firstTag = M05_M02_DEFAULT.replace("['x', 'y']", "['x']");
    return Stream.of(
        exactly("include _id only -> same as no projection", "{'_id': 1}", M05_M02_DEFAULT),
        exactly("exclude * -> empty documents", "{'*': 0}", "[{}, {}]"),
        exactly("$slice 1 -> first tag only", "{'tags': {'$slice': 1}}", firstTag));
  }

  // Reranks the frr_main documents with content (m06, m03, m07) on content, with a projection that
  // drops content. The docs (Parameters, rerankOn; projection examples) do not ask for the passage
  // field in the projection, the reranking design doc says it must be included; main reranks on
  // it anyway. The test pins current behavior; whether it is a bug is still under discussion.
  @ParameterizedTest(name = "{0}")
  @MethodSource(FILTER_PROJECTION_FACTORIES + "passageOutsideProjectionCases")
  default void passageFieldOutsideProjectionIsStillReranked(String request, String[] fields) {
    var response = postToFixture(MAIN, request);
    assertIds(response, "m06", "m03", "m07");
    assertFields(response, fields);
    var content = passages(MAIN, "content", "m06", "m03", "m07");
    assertRerankerCalls(defaultModelCalls(CHATGPT_QUERY, content));
  }

  static Stream<Arguments> passageOutsideProjectionCases() {
    String request =
        "{'findAndRerank': {'filter': {'content': {'$exists': true}}, 'projection': %s, 'sort':"
            + " {'$hybrid': {'$vectorize': 'ChatGPT upgraded'}}, 'options': {'rerankOn':"
            + " 'content'}}}";
    String[] noContent = {
      "_id grp title n", "_id grp title is_checked_out number_of_pages a", "_id grp title d"
    };
    String exclude = json(request.formatted("{'content': 0}"));
    return Stream.of(
        Arguments.of(
            Named.of("include title only -> no content", json(request.formatted("{'title': 1}"))),
            new String[] {"_id title", "_id title", "_id title"}),
        Arguments.of(Named.of("exclude content -> the other regular fields", exclude), noContent));
  }

  // Invalid projection definitions fail with the listed error while the command is built. The docs
  // do not specify these; the test pins current behavior so that any change is visible in review.
  @ParameterizedTest(name = "{0}")
  @MethodSource(FILTER_PROJECTION_FACTORIES + "invalidProjectionCases")
  default void invalidProjectionIsRejectedBeforeReading(
      Fixture fixture, String request, ErrorCode<?> code, String snippet) {
    assertApiError(postToFixture(fixture, request), code, snippet);
  }

  static Stream<Arguments> invalidProjectionCases() {
    return Stream.of(
        projection("projection is a string", "'abc'")
            .fails(UNSUPPORTED_PROJECTION_DEFINITION, "was String"),
        projection("projection is null", "null")
            .fails(UNSUPPORTED_PROJECTION_DEFINITION, "was Null"),
        projection("include then exclude", "{'title': 1, 'content': 0}")
            .fails(UNSUPPORTED_PROJECTION_PARAM, "cannot exclude 'content' on inclusion"),
        projection("path value is a string", "{'title': 'yes'}")
            .fails(UNSUPPORTED_PROJECTION_PARAM, "Number, Boolean or Object, was String"),
        projection("* next to another path", "{'*': 1, 'title': 1}")
            .fails(UNSUPPORTED_PROJECTION_PARAM, "('*') only allowed as the only root-level path"),
        projection("* with a string value", "{'*': 'x'}")
            .fails(UNSUPPORTED_PROJECTION_PARAM, "('*') value must be Number or Boolean"));
  }

  // Projects $similarity or $hybrid. Per the docs (Parameters, projection) included $ fields are
  // returned, and the reranking design doc says $hybrid can be asked for directly; main rejects
  // both. The test pins current behavior; whether it is a bug is still under discussion.
  @ParameterizedTest(name = "{0}")
  @MethodSource(FILTER_PROJECTION_FACTORIES + "otherDollarFieldProjectionCases")
  default void projectionOfOtherDollarFieldIsRejected(
      Fixture fixture, String request, ErrorCode<?> code, String snippet) {
    assertApiError(postToFixture(fixture, request), code, snippet);
  }

  static Stream<Arguments> otherDollarFieldProjectionCases() {
    return Stream.of(
        projection("include $similarity", "{'$similarity': 1}")
            .fails(UNSUPPORTED_PROJECTION_PARAM, "can start with '$' (path: '$similarity')"),
        projection("include $hybrid", "{'$hybrid': 1}")
            .fails(UNSUPPORTED_PROJECTION_PARAM, "can start with '$' (path: '$hybrid')"));
  }

  /** A case: description, collection and request, then what the request should give. */
  record FilterCase(String description, Fixture fixture, String request) {
    Arguments returns(List<Object> ids, ExpectedRerankCalls calls) {
      return Arguments.of(Named.of(description, fixture), request, ids, calls);
    }

    /** The reranker gets the frr_main passages of exactly these documents, returned in order. */
    Arguments reranksAndReturns(String... ids) {
      return returns(List.of((Object[]) ids), treeReranked(ids));
    }

    Arguments fails(ErrorCode<?> code, String snippet) {
      return Arguments.of(Named.of(description, fixture), request, code, snippet);
    }
  }

  private static FilterCase filter(String description, String filter) {
    return new FilterCase(description, MAIN, treeQuery(filter, null));
  }

  private static FilterCase projection(String description, String projection) {
    return new FilterCase(description, MAIN, treeQuery(null, projection));
  }

  private void assertEmptyPageWithoutReranking(String filter) {
    var request = findAndRerank().filter(json(filter)).hybrid(TREE_QUERY);
    var withVector = postToFixture(MAIN, request.option("includeSortVector", true).json());
    assertSuccessEnvelope(withVector.statusCode(200).body("data.documents", empty()), "sortVector");
    assertSortVector(withVector, 0.25, 0.25, 0.25, 0.25, 0.25);
    var plain = postToFixture(MAIN, treeQuery(filter, null)).statusCode(200);
    assertSuccessEnvelope(plain.body("data.documents", empty()));
    assertRerankerNotCalled();
  }

  /** The response succeeded and document i has exactly the space-separated field names at i. */
  private static void assertFields(ValidatableResponse response, String... fieldsPerDocument) {
    response.statusCode(200).body("errors", is(nullValue()));
    List<Map<String, Object>> documents = response.extract().jsonPath().getList("data.documents");
    assertThat(documents).hasSize(fieldsPerDocument.length);
    for (int i = 0; i < documents.size(); i++) {
      assertThat(documents.get(i).keySet())
          .as("fields of document %d", i)
          .containsExactlyInAnyOrder(fieldsPerDocument[i].split(" "));
    }
  }

  /** The $vectorize passages of these frr_main documents, for the official example query. */
  private static ExpectedRerankCalls treeReranked(String... ids) {
    return defaultModelCalls(TREE_QUERY, passages(MAIN, "$vectorize", ids));
  }

  /** All twelve $vectorize passages of frr_main, sent in a batch of ten and a batch of two. */
  private static ExpectedRerankCalls allTwelveReranked() {
    return treeReranked(
        IntStream.rangeClosed(1, 12).mapToObj("m%02d"::formatted).toArray(String[]::new));
  }

  /** What the reranker receives for twoDocs: m05 and m02 on $vectorize, in one batch. */
  private static ExpectedRerankCalls twoDocsReranked() {
    return defaultModelCalls(CHATGPT_QUERY, passages(MAIN, "$vectorize", "m05", "m02"));
  }

  /** The official example query on frr_main; filter and projection are single-quoted or null. */
  private static String treeQuery(String filter, String projection) {
    var request = findAndRerank();
    if (filter != null) {
      request.filter(json(filter));
    }
    if (projection != null) {
      request.projection(json(projection));
    }
    return request.hybrid(TREE_QUERY).json();
  }

  /** m05 and m02 of frr_main (vector ranks 2 and 1, rerank order m05, m02) with a projection. */
  private static String twoDocs(String projection, boolean includeScores) {
    var request = findAndRerank().filter(json("{'number_of_pages': {'$in': [120, 150]}}"));
    if (projection != null) {
      request.projection(json(projection));
    }
    String sort = json("{'$hybrid': {'$vectorize': '" + CHATGPT_QUERY + "'}}");
    return request.sort(sort).option("includeScores", includeScores).json();
  }

  private static Arguments shaped(String description, String projection, String fields) {
    return Arguments.of(Named.of(description, twoDocs(projection, true)), fields);
  }

  private static Arguments exactly(String description, String projection, String documents) {
    return Arguments.of(Named.of(description, twoDocs(projection, false)), documents);
  }

  /** Turns single quotes into double quotes, so the JSON in the cases needs no escaping. */
  private static String json(String singleQuoted) {
    return singleQuoted.replace('\'', '"');
  }
}
