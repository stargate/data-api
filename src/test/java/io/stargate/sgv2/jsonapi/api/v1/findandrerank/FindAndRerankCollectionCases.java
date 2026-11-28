package io.stargate.sgv2.jsonapi.api.v1.findandrerank;

import static io.stargate.sgv2.jsonapi.api.v1.AbstractKeyspaceIntegrationTestBase.TEST_PROP_LEXICAL_DISABLED;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankAssertions.*;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.*;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankRequests.*;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankSortCases.*;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.RerankerAssertions.*;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.restassured.response.ValidatableResponse;
import io.stargate.sgv2.jsonapi.exception.*;
import io.stargate.sgv2.jsonapi.testresource.FakeRerankerTestResource.Model;
import java.util.List;
import java.util.Map;
import java.util.function.*;
import java.util.stream.Stream;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;

/**
 * findAndRerank tests of the collection settings (vector, vectorize, lexical, rerank, indexing,
 * analyzers, older table comments) and of how documents were written ({@code $hybrid} and others).
 *
 * <p>For review: JSON, case names and static helpers are those of {@link FindAndRerankSortCases}.
 * Four tests rewrite a table comment with CQL to reach states the API cannot create; they depend on
 * its internal format and restore it in {@code finally}. createCollection cases that succeed create
 * and drop frr_tmp_analyzer; failing ones name frr_tmp_invalid, which is never created.
 */
public interface FindAndRerankCollectionCases extends FindAndRerankTestContext {
  String CASES = "io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankCollectionCases#";
  String VECTORIZE_SORT = "{'$hybrid': {'$vectorize': 'ChatGPT upgraded'}}";
  String VECTORIZE_AND_LEXICAL = "{'$hybrid': {'$vectorize': 'ChatGPT upgraded', '$lexical': 'x'}}";
  List<String> LEXICAL_AND_RERANK = List.of("lexical", "rerank");

  // Creates a collection with invalid rerank options and expects INVALID_CREATE_COLLECTION_OPTIONS
  // with the message of the same override check, or the code in the case name. The docs do not
  // specify this; the test pins current behavior so that any change is visible in review.
  @ParameterizedTest(name = "{0}")
  @MethodSource(CASES + "badRerank")
  default void createCollectionWithInvalidRerankOptionsFails(
      String rerank, ErrorCode<?> code, String snippet) {
    assertApiError(postToKeyspace(createCommand("{'rerank': " + rerank + "}")), code, snippet);
  }

  static Stream<Arguments> badRerank() {
    String nvidia = model(Model.DEFAULT);
    String disabled = "{'enabled': false, 'service': {" + nvidia + "}}";
    String serviceGiven = "'rerank' is disabled, but 'rerank.service' configuration is provided";
    String unknownProvider = "Reranking provider 'x' is not supported";
    String jinaDisabled = "Reranking provider 'jinaAI' is disabled";
    String unknownModel = "Model 'm' is not supported by reranking provider 'nvidia'";
    String auth = nvidia + ", 'authentication': {'providerKey': 'k'}";
    String noneOrHeader = "Reranking provider 'nvidia' currently only supports 'NONE' or 'HEADER'";
    String parameters = nvidia + ", 'parameters': {'a': 1}";
    String noParameters = "Reranking provider 'nvidia' currently doesn't support any parameters";
    return Stream.of(
        invalidRerank("rerank disabled with a service", disabled, serviceGiven),
        invalidRerank("rerank without enabled", "{}", "'enabled' is required property"),
        invalidService("service without provider", "'modelName': 'm'", "Provider name is required"),
        invalidService("service with unknown provider", "'provider': 'x'", unknownProvider),
        invalidService("service with disabled provider", model(Model.JINA), jinaDisabled),
        invalidService("service without model", "'provider': 'nvidia'", "Model name is required"),
        invalidService(
            "service with unknown model", "'provider': 'nvidia', 'modelName': 'm'", unknownModel),
        invalidService("service with authentication", auth, noneOrHeader),
        invalidService("service with parameters", parameters, noParameters),
        args(
            "rerank model is DEPRECATED -> DEPRECATED_AI_MODEL",
            service(model(Model.DEPRECATED)),
            SchemaException.Code.DEPRECATED_AI_MODEL,
            "The model is: nvidia/a-random-deprecated-model. It is at DEPRECATED status."),
        args(
            "rerank model is END_OF_LIFE -> END_OF_LIFE_AI_MODEL",
            service(model(Model.EOL)),
            SchemaException.Code.END_OF_LIFE_AI_MODEL,
            "The model is: nvidia/a-random-EOL-model. It is at END_OF_LIFE status."));
  }

  // Creates a collection whose lexical option is not an object (REQUEST_STRUCTURE_MISMATCH) or
  // whose analyzer is not a string or an object (INVALID_CREATE_COLLECTION_OPTIONS); expects the
  // error to name the type, on both backends. Per the BM25 design doc (Modified Command -
  // createCollection), lexical is an object and its analyzer a string or an object.
  @ParameterizedTest(name = "{0}")
  @MethodSource(CASES + "badLexicalOption")
  default void createCollectionWithLexicalOfWrongTypeFails(
      String lexical, ErrorCode<?> code, String[] snippets) {
    assertApiError(postToKeyspace(createCommand("{'lexical': " + lexical + "}")), code, snippets);
  }

  static Stream<Arguments> badLexicalOption() {
    var mismatch = RequestException.Code.REQUEST_STRUCTURE_MISMATCH;
    String[] desc = {"valid JSON but has a structural mismatch:", "LexicalDesc"};
    return Stream.of(
        args("lexical is 'yes'", "'yes'", mismatch, desc),
        args("lexical is ''", "''", mismatch, desc),
        args("lexical is 1", "1", mismatch, desc),
        args("lexical is true", "true", mismatch, desc),
        args("lexical is []", "[]", mismatch, desc),
        wrongAnalyzer("analyzer is 5", "5", "Number"),
        wrongAnalyzer("analyzer is true", "true", "Boolean"),
        wrongAnalyzer("analyzer is [1, 2, 3]", "[1, 2, 3]", "Array"));
  }

  // Creates a collection with lexical options the BM25 design doc (Modified Command -
  // createCollection) allows: no "enabled", which it says defaults to true, or an analyzer object,
  // whose fields it does not list (the brainstorm design doc defers analyzer objects). Main fails
  // with INVALID_CREATE_COLLECTION_OPTIONS on both backends. The test pins current behavior;
  // whether it is a bug is still under discussion.
  @ParameterizedTest(name = "{0}")
  @MethodSource(CASES + "designDocLexical")
  default void createCollectionWithLexicalOptionsFromTheDesignDocFails(
      String lexical, String message) {
    var code = SchemaException.Code.INVALID_CREATE_COLLECTION_OPTIONS;
    String snippet = "'createCollection' command option(s) invalid: " + message;
    assertApiError(postToKeyspace(createCommand("{'lexical': " + lexical + "}")), code, snippet);
  }

  static Stream<Arguments> designDocLexical() {
    String noEnabled = "'enabled' is required property for 'lexical' Object value";
    String fields = " for 'lexical.analyzer'. Valid fields are: [charFilters, filters, tokenizer]";
    String type = " property of 'lexical.analyzer' must be JSON ";
    return Stream.of(
        args("lexical is {}", "{}", noEnabled),
        args("lexical has an analyzer but no enabled", "{'analyzer': 'standard'}", noEnabled),
        args(
            "analyzer has one unknown field",
            withAnalyzer("{'tokeniser': {'name': 'standard'}}"),
            "Invalid field" + fields + ", found: [tokeniser]"),
        args(
            "analyzer has two unknown fields",
            withAnalyzer("{'tokeniser': {'name': 'standard'}, 'extra': 123}"),
            "Invalid fields" + fields + ", found: [extra, tokeniser]"),
        // Sent as filter, extra, which is also their hash set order, so found must be sorted.
        args(
            "analyzer has unknown fields filter and extra",
            withAnalyzer("{'filter': [], 'extra': 1}"),
            "Invalid fields" + fields + ", found: [extra, filter]"),
        args(
            "analyzer tokenizer is a string",
            withAnalyzer("{'tokenizer': 'standard'}"),
            "'tokenizer'" + type + "Object, is: String"),
        args(
            "analyzer filters is an object",
            withAnalyzer("{'filters': {}}"),
            "'filters'" + type + "Array, is: Object"),
        args(
            "analyzer charFilters is a string",
            withAnalyzer("{'charFilters': 'x'}"),
            "'charFilters'" + type + "Array, is: String"));
  }

  // Creates a collection with lexical off and an analyzer, on both backends. Expects null and {}
  // to be accepted and dropped, so that the stored setting is {"enabled": false}, and any other
  // value to fail with INVALID_CREATE_COLLECTION_OPTIONS naming its JSON type. The docs do not
  // specify this; the test pins current behavior so that any change is visible in review.
  @ParameterizedTest(name = "{0}")
  @MethodSource(CASES + "analyzerWhileOff")
  default void createCollectionWithLexicalOffAcceptsOnlyAnEmptyAnalyzer(
      String analyzer, String type) {
    String lexical = "{'enabled': false, 'analyzer': " + analyzer + "}";
    if (type == null) {
      assertThat(storedLexical(lexical)).isEqualTo(parse("{\"enabled\": false}"));
      return;
    }
    var code = SchemaException.Code.INVALID_CREATE_COLLECTION_OPTIONS;
    String snippet = "'lexical.analyzer' property was provided with an unexpected type: " + type;
    assertApiError(postToKeyspace(createCommand("{'lexical': " + lexical + "}")), code, snippet);
  }

  static Stream<Arguments> analyzerWhileOff() {
    return Stream.of(
        Arguments.of(Named.of("analyzer null -> accepted, not stored", "null"), null),
        Arguments.of(Named.of("analyzer {} -> accepted, not stored", "{}"), null),
        args("analyzer 'standard'", "'standard'", "String"),
        args("analyzer ''", "''", "String"),
        args("analyzer []", "[]", "Array"),
        args("analyzer 1", "1", "Number"),
        args("analyzer true", "true", "Boolean"),
        args("analyzer object", "{'tokenizer': {'name': 'standard'}}", "Object"));
  }

  // Creates a collection with lexical on and the analyzer "STANDARD" (the brainstorm design doc's
  // name), none, null or {}. On HCD expects "STANDARD" kept and "standard" otherwise; on DSE
  // LEXICAL_FEATURE_NOT_ENABLED. The BM25 design doc uses "standard"; neither says what null or {}
  // means. The test pins current behavior; whether it is a bug is still under discussion.
  @ParameterizedTest(name = "{0}")
  @MethodSource(CASES + "storedAnalyzer")
  default void createCollectionWithLexicalOnStoresTheAnalyzer(String analyzer, String stored) {
    String lexical = "{'enabled': true" + analyzer + "}";
    if (lexicalAvailable()) {
      String expected = "{'enabled': true, 'analyzer': '" + stored + "'}";
      assertThat(storedLexical(lexical)).isEqualTo(parse(json(expected)));
      return;
    }
    var code = SchemaException.Code.LEXICAL_FEATURE_NOT_ENABLED;
    String snippet = "lexical search is not supported by this database";
    assertApiError(postToKeyspace(createCommand("{'lexical': " + lexical + "}")), code, snippet);
  }

  static Stream<Arguments> storedAnalyzer() {
    return Stream.of(
        args("analyzer 'STANDARD' -> stored unchanged", ", 'analyzer': 'STANDARD'", "STANDARD"),
        args("no analyzer -> 'standard'", "", "standard"),
        args("analyzer null -> 'standard'", ", 'analyzer': null", "standard"),
        args("analyzer {} -> 'standard'", ", 'analyzer': {}", "standard"));
  }

  // Creates frr_novector again with its lexical setting and only the rerank setting changed, and
  // expects EXISTING_COLLECTION_DIFFERENT_SETTINGS with the table comment unchanged. Per the hybrid
  // search docs (Run a hybrid search with the Data API), collection settings cannot change after
  // creation. The docs do not say what repeating the same setting does; it succeeds. Nor do they
  // say if an empty authentication or parameters object equals none; it is a different setting.
  @ParameterizedTest(name = "{0}")
  @MethodSource(CASES + "sameRerank")
  default void createCollectionAgainOnlySucceedsWithTheSameRerank(String rerank, boolean same) {
    String collection = ensure(NOVECTOR);
    String comment = TableCommentRewriter.readComment(keyspace(), collection);
    String options = "{'lexical': {'enabled': false}, 'rerank': " + rerank + "}";
    var response = postToKeyspace(createCommand(collection, options));
    assertThat(TableCommentRewriter.readComment(keyspace(), collection)).isEqualTo(comment);
    if (same) {
      response.statusCode(200).body("status.ok", is(1)).body("errors", is(nullValue()));
      assertRerankerNotCalled();
      return;
    }
    var code = SchemaException.Code.EXISTING_COLLECTION_DIFFERENT_SETTINGS;
    String snippet = "Collection '%s' already exists but with settings different from ones passed";
    assertApiError(response, code, snippet.formatted(collection));
  }

  static Stream<Arguments> sameRerank() {
    String nvidia = model(Model.DEFAULT);
    String voyage = service(model(Model.VOYAGE));
    String emptyAuthentication = service(nvidia + ", 'authentication': {}");
    String emptyParameters = service(nvidia + ", 'parameters': {}");
    String nulls = service(nvidia + ", 'authentication': null, 'parameters': null");
    return Stream.of(
        args("rerank switched to another model", service(model(Model.SECOND)), false),
        args("rerank switched off", "{'enabled': false}", false),
        args("rerank provider switched to voyageAI, same model name", voyage, false),
        args("rerank default model with an empty authentication", emptyAuthentication, false),
        args("rerank default model with empty parameters", emptyParameters, false),
        args("rerank on with the default model -> accepted", "{'enabled': true}", true),
        args("rerank default model as a full service -> accepted", service(nvidia), true),
        args("rerank default model, null authentication and parameters -> accepted", nulls, true));
  }

  // Creates a collection whose rerank service is the default model plus an empty authentication or
  // parameters object. Expects the table comment to store {} (null for the other field) and
  // findCollections with explain to show {} without the null field. The docs do not say if an empty
  // object equals none; the test pins current behavior so that any change is visible in review.
  @ParameterizedTest(name = "{0}")
  @MethodSource(CASES + "emptyServiceObject")
  default void createCollectionKeepsAnEmptyRerankServiceObject(String rerank, String stored) {
    String collection = TMP_ANALYZER.name();
    try {
      String options = "{'lexical': {'enabled': false}, 'rerank': " + rerank + "}";
      postToKeyspace(createCommand(collection, options))
          .statusCode(200)
          .body("errors", is(nullValue()))
          .body("status.ok", is(1));
      var comment = parse(TableCommentRewriter.readComment(keyspace(), collection));
      assertThat(comment.at("/collection/options/rerank")).isEqualTo(parse(json(stored)));
      String shown = "status.collections.find { it.name == '%s' }.options.rerank";
      postToKeyspace(json("{'findCollections': {'options': {'explain': true}}}"))
          .statusCode(200)
          .body("errors", is(nullValue()))
          .body(shown.formatted(collection), jsonEquals(json(rerank)));
    } finally {
      postToKeyspace(json("{'deleteCollection': {'name': '" + collection + "'}}")).statusCode(200);
    }
    assertRerankerNotCalled();
  }

  static Stream<Arguments> emptyServiceObject() {
    String nvidia = model(Model.DEFAULT);
    return Stream.of(
        args(
            "new collection, rerank authentication {} -> stored and shown as {}",
            service(nvidia + ", 'authentication': {}"),
            service(nvidia + ", 'authentication': {}, 'parameters': null")),
        args(
            "new collection, rerank parameters {} -> stored and shown as {}",
            service(nvidia + ", 'parameters': {}"),
            service(nvidia + ", 'authentication': null, 'parameters': {}")));
  }

  // Sends a sort the collection cannot serve; expects UNSUPPORTED_VECTOR_SORT_FOR_COLLECTION on
  // frr_novector, UNSUPPORTED_VECTORIZE_SORT_FOR_COLLECTION on frr_byo and frr_novectorize, and
  // UNSUPPORTED_RERANKING_COMMAND on frr_norerank. Per the findAndRerank docs (introduction;
  // Parameters, sort), it needs vector and rerank (or an override); $vectorize needs vectorize.
  @ParameterizedTest(name = "{0}")
  @MethodSource(CASES + "missingFeature")
  default void collectionWithoutRequiredFeatureRejectsTheSort(
      String hybrid, Fixture fixture, ErrorCode<?> code, String snippet) {
    String request = request("{'$hybrid': " + hybrid + "}", Q_AND_ON);
    assertApiError(postToFixture(fixture, request), code, snippet);
  }

  static Stream<Arguments> missingFeature() {
    String vectorize = "{'$vectorize': 'a'}";
    String binary = "{'$vector': {'$binary': '" + binaryOf(0.1f, 0.2f, 0.3f) + "'}}";
    String both = "{'$vectorize': 'a', '$vector': [0.1, 0.2, 0.3]}";
    String nullVector = "{'$vectorize': 'a', '$vector': null}";
    var noRerank = RequestException.Code.UNSUPPORTED_RERANKING_COMMAND;
    String noOverride = "a reranking service override was not provided with the command";
    return Stream.of(
        noVector("$hybrid string on frr_novector", "'a'"),
        noVector("$vectorize on frr_novector", vectorize),
        noVector("$vectorize and $lexical on frr_novector", "{'$vectorize': 'a', '$lexical': 'a'}"),
        noVector("$vector on frr_novector", "{'$vector': [0.1, 0.2, 0.3]}"),
        noVector("$binary $vector on frr_novector", binary),
        noVector("$vectorize and $vector on frr_novector", both),
        noVectorize("$hybrid string on frr_byo", BYO, "'a'"),
        noVectorize("$vectorize on frr_novectorize", NOVECTORIZE, vectorize),
        noVectorize("$vectorize, null $vector on frr_novectorize", NOVECTORIZE, nullVector),
        noVectorize("$vectorize and $vector on frr_novectorize", NOVECTORIZE, both),
        args("no rerank override on frr_norerank", vectorize, NORERANK, noRerank, noOverride));
  }

  // Sends an explicit $lexical to the collection without lexical and expects
  // LEXICAL_NOT_ENABLED_FOR_COLLECTION before any other check. The docs require lexical only for
  // hybrid search, and the existing-behavior design doc says the lexical read can be skipped. The
  // test pins current behavior; whether it is a bug is still under discussion.
  @ParameterizedTest(name = "{0}")
  @MethodSource(CASES + "explicitLexical")
  default void explicitLexicalOnCollectionWithoutLexicalFails(String request) {
    String collection = ensure(NOLEX);
    var code = SchemaException.Code.LEXICAL_NOT_ENABLED_FOR_COLLECTION;
    String snippet = "The collection without a lexical index: " + keyspace() + "." + collection;
    assertApiError(postToCollection(collection, request), code, snippet);
  }

  static Stream<Named<String>> explicitLexical() {
    String vector = "{'$hybrid': {'$vector': [0.1, 0.2, 0.3, 0.4, 0.5], '$lexical': 'a'}}";
    String lexical = "{'$hybrid': {'$lexical': 'a'}}";
    return Stream.of(
        Named.of("$vectorize and $lexical", request(VECTORIZE_AND_LEXICAL, null)),
        Named.of("$vector and $lexical", request(vector, Q_AND_ON)),
        Named.of("only $lexical", request(lexical, Q_AND_ON)),
        Named.of("only $lexical, no rerankQuery", request(lexical, "{'rerankOn': 'title'}")),
        Named.of("only $lexical, no rerankOn", request(lexical, "{'rerankQuery': 'q'}")));
  }

  // Sends {"$hybrid": "ChatGPT upgraded"} to the collection without lexical, and expects only a
  // vector read of 50, a null $bm25Rank and only the vector part in $rrf. The findAndRerank docs
  // require lexical for hybrid search and say the reads default to limit. The test pins current
  // behavior; whether it is a bug is still under discussion.
  @Test
  default void hybridStringOnCollectionWithoutLexicalSkipsTheBm25Read() {
    var request = findAndRerank().hybrid("ChatGPT upgraded").option("includeScores", true);
    var response = postToFixture(NOLEX, request.json(), tracingHeaders());
    assertIds(response, "n2", "n1");
    assertScores(
        response,
        scores(3.25f, 0.93070626f, 2, null, rrf(2)),
        scores(1.5f, 0.9787127f, 1, null, rrf(1)));
    assertRerankerCalls(calls(1).query("ChatGPT upgraded").passages(SNEAKERS, NEW_DATA));
    assertTracedReads(response, "ANN 50");
  }

  // Sends a $vector sort to the collection with rerank {"enabled": true} and no service, and an
  // indexing allow list without $vector. Expects the default model. Per the brainstorm design doc
  // (DDL Create, Collection), rerank without a service uses the default model.
  @Test
  default void rerankEnabledWithoutServiceUsesTheDefaultModel() {
    var response = postToFixture(IDS, idsRequest("nomatch", "{'$vector': " + IDS_VECTOR + "}"));
    assertIds(response, "nm-miss", "nm-hit");
    assertScores(
        response,
        scores(1.375f, 0.8535534f, 2, null, rrf(2)),
        scores(0.875f, 1.0f, 1, null, rrf(1)));
    assertRerankerCalls(calls(1).query("kiwi").passages("Has the term", "Lacks the term"));
  }

  // On HCD, sends $lexical "kiwi" to the collection with an analyzer object (Porter stemming) and
  // an allow list, and expects the BM25 read to find "The kiwis were ripening". The BM25 design doc
  // allows an analyzer object; the brainstorm design doc defers it. The test pins current behavior;
  // whether it is a bug is still under discussion.
  @Test
  @DisabledIfSystemProperty(named = TEST_PROP_LEXICAL_DISABLED, matches = "true")
  default void analyzerObjectAndAllowListSupportTheBm25Read() {
    String hybrid = "{'$vector': " + IDS_VECTOR + ", '$lexical': 'kiwi'}";
    var response = postToFixture(IDS, idsRequest("analyzer", hybrid));
    assertIds(response, "an1");
    assertScores(response, scores(1.125f, 1.0f, 1, 1, rrf(1, 1)));
    assertRerankerCalls(calls(1).query("kiwi").passages("Stemmed words"));
  }

  // On HCD, finds documents inserted with a $hybrid string, a $hybrid object, and $vectorize plus
  // $lexical by a word of their text, and expects rank 1 in both reads. Per the hybrid search docs
  // (Run a hybrid search with the Data API), each of the three ways fills $vectorize and $lexical.
  @Test
  @DisabledIfSystemProperty(named = TEST_PROP_LEXICAL_DISABLED, matches = "true")
  default void documentsWrittenForHybridSearchAreFoundByBothReads() {
    assertFoundByBothReads("m06", "Harbor lights", "harbor", 1.875f, 1f);
    assertFoundByBothReads("m07", "Island charts", "islands", -1.25f, 1f);
    assertFoundByBothReads("m01", "Quilt for dreamers", "grassy", -0.75f, 0.87512046f);
  }

  // Inserts documents with "$hybrid": null and with $hybrid objects that have only $vectorize or
  // (HCD) only $lexical, the other field left out or set to null. Expects the null one never to be
  // found, the first object to have no lexical text and the second no vector. The docs do not
  // specify this; the test pins current behavior so that any change is visible in review.
  @ParameterizedTest(name = "{0}")
  @MethodSource(CASES + "otherField")
  default void hybridNullOrSingleFieldObjectLeavesTheOtherFieldEmpty(boolean setToNull) {
    boolean lexical = lexicalAvailable();
    String docs =
        "{'_id': 't-null', 'grp': 'tmp', 'title': 'Harbor lights', '$hybrid': null},"
            + "{'_id': 't-vec', 'grp': 'tmp', 'title': 'Island charts',"
            + " '$hybrid': {'$vectorize': 'Updating new data'"
            + (setToNull ? ", '$lexical': null}}" : "}}")
            + (lexical
                ? ", {'_id': 't-lex', 'grp': 'tmp', 'title': 'Pumpkin soup',"
                    + " '$hybrid': {'$lexical': 'updating grassy'"
                    + (setToNull ? ", '$vectorize': null}}" : "}}")
                : "");
    String collection = ensure(MAIN);
    try {
      postToCollection(collection, json("{'insertMany': {'documents': [" + docs + "]}}"))
          .body("errors", is(nullValue()))
          .body("status.insertedIds", hasSize(lexical ? 3 : 2));
      String lexicalSort = lexical ? ", '$lexical': 'updating'" : "";
      String sort = "{'$hybrid': {'$vectorize': 'Updating new data'" + lexicalSort + "}}";
      String options = "{'rerankOn': 'title', 'includeScores': true}";
      var response = postToCollection(collection, request("{'grp': 'tmp'}", sort, options));
      var vectorOnly = scores(-1.125f, 1.0f, 1, null, rrf(1));
      var expected = calls(1).query("Updating new data");
      if (lexical) {
        assertIds(response, "t-lex", "t-vec");
        assertScores(response, scores(1.625f, -2147483648f, null, 1, rrf(1)), vectorOnly);
        assertRerankerCalls(expected.passages("Pumpkin soup", "Island charts"));
      } else {
        assertIds(response, "t-vec");
        assertScores(response, vectorOnly);
        assertRerankerCalls(expected.passages("Island charts"));
      }
    } finally {
      postToCollection(collection, json("{'deleteMany': {'filter': {'grp': 'tmp'}}}"))
          .statusCode(200);
    }
  }

  static Stream<Named<Boolean>> otherField() {
    return Stream.of(
        Named.of("other field left out", false), Named.of("other field set to null", true));
  }

  // Inserts a document with a bad $hybrid, also as the second document of an insertMany; expects
  // HYBRID_FIELD_CONFLICT next to another field, HYBRID_FIELD_UNSUPPORTED_VALUE_TYPE for an array,
  // HYBRID_FIELD_UNKNOWN_SUBFIELDS for unknown keys (listed in request order),
  // HYBRID_FIELD_UNSUPPORTED_SUBFIELD_VALUE_TYPE for a non-string, or the code in the case name,
  // and no document stored. The docs do not specify this; the test pins current behavior.
  @ParameterizedTest(name = "{0}")
  @MethodSource(CASES + "badHybrid")
  default void documentWithBadHybridFieldIsRejected(
      String command, Fixture fixture, ErrorCode<?> code, String snippet) {
    var response = postToFixture(fixture, command);
    if (code == SchemaException.Code.LEXICAL_NOT_ENABLED_FOR_COLLECTION) {
      assertWriteError(response, code, snippet); // found when the document is shredded
    } else {
      assertApiError(response, code, snippet); // found before the insert starts
    }
    assertNothingStored(fixture);
  }

  static Stream<Arguments> badHybrid() {
    var valueType = RequestException.Code.HYBRID_FIELD_UNSUPPORTED_VALUE_TYPE;
    var unknownKey = RequestException.Code.HYBRID_FIELD_UNKNOWN_SUBFIELDS;
    var subfield = RequestException.Code.HYBRID_FIELD_UNSUPPORTED_SUBFIELD_VALUE_TYPE;
    String withVectorize = "'$hybrid': {'$lexical': 'a'}, '$vectorize': 'b'";
    String foo = "'$hybrid': {'$vectorize': 'a', 'foo': 1}";
    String fooBar = "'$hybrid': {'$vectorize': 'a', 'foo': 1, 'bar': 2}";
    String trueVectorize = "'$hybrid': {'$vectorize': true}";
    String lexicalNumber = "'$hybrid': {'$lexical': 1}";
    String array = "expected String, Object or `null` but received Array (Document 1 of 1)";
    String unknown =
        "expected '$lexical' and/or '$vectorize' but encountered: 'foo' (Document 1 of 1)";
    String unknowns =
        "expected '$lexical' and/or '$vectorize' but encountered: 'foo', 'bar' (Document 1 of 1)";
    String number = "for '$lexical' but received Number";
    String bool = "for '$vectorize' but received Boolean";
    String second = " (Document 2 of 2)";
    return Stream.of(
        args(
            "$hybrid string, collection without lexical -> LEXICAL_NOT_ENABLED_FOR_COLLECTION",
            insertOne("'$hybrid': 'some text'"),
            NOLEX,
            SchemaException.Code.LEXICAL_NOT_ENABLED_FOR_COLLECTION,
            "The collection without a lexical index"),
        conflict("$hybrid string next to $lexical", "'$hybrid': 'a', '$lexical': 'b'"),
        conflict("null $hybrid next to $vector", "'$hybrid': null, '$vector': [0.1, 0.2, 0.3]"),
        conflict("$hybrid object next to $vectorize", withVectorize),
        args("$hybrid is an array", insertOne("'$hybrid': ['a']"), MAIN, valueType, array),
        args("$hybrid has an unknown key", insertOne(foo), MAIN, unknownKey, unknown),
        args("$hybrid has two unknown keys", insertOne(fooBar), MAIN, unknownKey, unknowns),
        args("$hybrid.$lexical is a number", insertOne(lexicalNumber), MAIN, subfield, number),
        args("$hybrid.$vectorize is a boolean", insertOne(trueVectorize), MAIN, subfield, bool),
        args(
            "insertMany, second document's $hybrid is an array",
            insertTwo("'$hybrid': ['a']"),
            MAIN,
            valueType,
            "but received Array" + second),
        args(
            "insertMany, second document's $hybrid has an unknown key",
            insertTwo(foo),
            MAIN,
            unknownKey,
            "encountered: 'foo'" + second),
        args(
            "insertMany, second document's $hybrid.$lexical is a number",
            insertTwo(lexicalNumber),
            MAIN,
            subfield,
            number + second));
  }

  // Sends $hybrid in an upserting findOneAndReplace replacement and updateOne $set. The hybrid
  // search docs show $hybrid only for inserts; the brainstorm design doc calls it a shorthand for
  // $vectorize and $lexical. Main expands it only in inserts: SHRED_BAD_FIELD_NAME, nothing stored.
  // The test pins current behavior; whether it is a bug is still under discussion.
  @ParameterizedTest(name = "{0}")
  @MethodSource(CASES + "otherWrites")
  default void hybridInOtherWriteCommandsIsNotExpanded(String command) {
    var code = DocumentException.Code.SHRED_BAD_FIELD_NAME;
    assertApiError(postToFixture(MAIN, command), code, "field name '$hybrid' starts with '$'");
    assertNothingStored(MAIN);
  }

  static Stream<Named<String>> otherWrites() {
    String filter = "'filter': {'_id': 'bad'}, ";
    String upsert = ", 'options': {'upsert': true}";
    String replacement = "'replacement': {'_id': 'bad', '$hybrid': 'some text'}";
    String set = "'update': {'$set': {'$hybrid': 'some text'}}";
    return Stream.of(
        Named.of(
            "findOneAndReplace with a $hybrid string",
            json("{'findOneAndReplace': {" + filter + replacement + upsert + "}}")),
        Named.of(
            "updateOne setting a $hybrid string",
            json("{'updateOne': {" + filter + set + upsert + "}}")));
  }

  // Inserts a document whose $vectorize or (HCD) $lexical is not a string, or whose $lexical is not
  // at the top level. Expects INVALID_VECTORIZE_VALUE_TYPE for $vectorize before the insert starts,
  // or the write error SHRED_BAD_DOCUMENT_LEXICAL_TYPE or the code in the case name. Per the
  // hybrid search docs (Run a hybrid search with the Data API) both fields are strings, and per the
  // BM25 design doc $lexical is a top-level field.
  @ParameterizedTest(name = "{0}")
  @MethodSource(CASES + "badLexical")
  default void documentWithInvalidLexicalOrVectorizeIsRejected(
      String fields, ErrorCode<?> code, String snippet) {
    if (code == DocumentException.Code.INVALID_VECTORIZE_VALUE_TYPE) {
      assertApiError(postToFixture(MAIN, insertOne(fields)), code, snippet);
      return;
    }
    Assumptions.assumeTrue(lexicalAvailable(), "needs lexical search (HCD)");
    assertWriteError(postToFixture(MAIN, insertOne(fields)), code, snippet);
  }

  static Stream<Arguments> badLexical() {
    var type = DocumentException.Code.SHRED_BAD_DOCUMENT_LEXICAL_TYPE;
    var name = DocumentException.Code.SHRED_BAD_FIELD_NAME;
    var vectorize = DocumentException.Code.INVALID_VECTORIZE_VALUE_TYPE;
    String text = "the value for field '$lexical' must be a JSON String, not a JSON ";
    String sub = "'sub': {'$lexical': 'x'}";
    String dollar = "field name '$lexical' starts with '$'";
    String vtext = "Invalid $vectorize value: needs to be String, not ";
    return Stream.of(
        args("$lexical is a number", "'$lexical': 1", type, text + "Number"),
        args("$lexical is a boolean", "'$lexical': true", type, text + "Boolean"),
        args("$lexical is an object", "'$lexical': {'a': 'x'}", type, text + "Object"),
        args("$lexical is an array", "'$lexical': ['x']", type, text + "Array"),
        args("$lexical inside a subdocument -> SHRED_BAD_FIELD_NAME", sub, name, dollar),
        args("$vectorize is a number", "'$vectorize': 1", vectorize, vtext + "Number"),
        args("$vectorize is a boolean", "'$vectorize': true", vectorize, vtext + "Boolean"),
        args("$vectorize is an object", "'$vectorize': {'a': 'x'}", vectorize, vtext + "Object"),
        args("$vectorize is an array", "'$vectorize': ['x']", vectorize, vtext + "Array"));
  }

  // Sends a $vectorize sort to the seven documents whose texts are not among the six sentences the
  // test embedding provider knows. Expects them and the query to share one vector, so every $vector
  // score is 1.0, the vector ranks are 1 to 7 in an order the database picks, and rerank scores
  // alone set the order. The docs do not specify this; the test pins current behavior.
  @Test
  default void textsOutsideTheSampleSentencesShareOneVector() {
    String sort = "{'$hybrid': {'$vectorize': 'A tree in the woods'}}";
    String options = "{'includeScores': true, 'includeSortVector': true}";
    var response = postToFixture(MAIN, request("{'grp': 'plain'}", sort, options));
    assertIds(response, "m08", "m11", "m06", "m12", "m09", "m07", "m10");
    String[] texts = passages(MAIN, "$vectorize", "m06", "m07", "m08", "m09", "m10", "m11", "m12");
    assertRerankerCalls(calls(1).query("A tree in the woods").passages(texts));
    assertSortVector(response, 0.25, 0.25, 0.25, 0.25, 0.25);
    List<Map<String, Number>> scores =
        response.extract().jsonPath().getList("status.documentResponses.scores");
    assertThat(scores)
        .extracting(s -> s.get("$rerank").floatValue())
        .containsExactly(3.75f, 2.75f, 1.875f, 1.0f, 0.25f, -1.25f, -2.5f);
    var vectorRanks = scores.stream().map(s -> s.get("$vectorRank").intValue()).toList();
    assertThat(vectorRanks).containsExactlyInAnyOrder(1, 2, 3, 4, 5, 6, 7);
    for (var s : scores) {
      assertThat(s.get("$vector").floatValue()).isCloseTo(1f, within(1e-6f));
      assertThat(s.get("$bm25Rank")).isNull();
      float rrf = rrf(s.get("$vectorRank").intValue());
      assertThat(s.get("$rrf").floatValue()).isCloseTo(rrf, within(1e-6f));
    }
  }

  // Rewrites the table comment to the pre-lexical format; expects UNSUPPORTED_RERANKING_COMMAND and
  // LEXICAL_NOT_ENABLED_FOR_COLLECTION as the hybrid search docs say, and success with a valid
  // override, which the findAndRerank docs and issue #2459 allow but the hybrid search docs do not.
  // The test pins current behavior; whether it is a bug is still under discussion.
  @Test
  default void preLexicalCollectionWorksOnlyWithARerankOverride() {
    var refused = RequestException.Code.UNSUPPORTED_RERANKING_COMMAND;
    var noLexical = SchemaException.Code.LEXICAL_NOT_ENABLED_FOR_COLLECTION;
    withRewrittenComment(
        COMMENT,
        comment -> {
          var settings = (ObjectNode) comment.get("collection");
          settings.put("schema_version", 1);
          ((ObjectNode) settings.get("options")).remove(LEXICAL_AND_RERANK);
        },
        VECTORIZE_SORT,
        response -> {
          assertApiError(response, refused, "a reranking service override was not provided");
          var lexical = postToFixture(COMMENT, request(VECTORIZE_AND_LEXICAL, null));
          assertApiError(lexical, noLexical, "The collection without a lexical index");
          String override = "{'rerank': {" + model(Model.SECOND) + "}}";
          assertIds(postToFixture(COMMENT, request(VECTORIZE_SORT, override)), "k2", "k1");
          String[] texts = passages(COMMENT, "$vectorize", "k1", "k2");
          assertRerankerCalls(calls(1).on(Model.SECOND).query("ChatGPT upgraded").passages(texts));
        });
  }

  // Rewrites the rerank or lexical setting in a table comment into one the API never writes: on
  // with no service or analyzer, or off with one. Expects UNEXPECTED_SERVER_ERROR naming the
  // exception, with and without a rerank override. No document covers a broken stored setting.
  // The test pins current behavior; whether it is a bug is still under discussion.
  @ParameterizedTest(name = "{0}")
  @MethodSource(CASES + "brokenSetting")
  default void brokenSettingInTableCommentGivesServerError(
      Fixture fixture, String option, Consumer<ObjectNode> edit, String error) {
    var code = ServerException.Code.UNEXPECTED_SERVER_ERROR;
    String snippet = "Error Class: " + error.formatted(keyspace(), fixture.name());
    String override = "{'rerank': {" + model(Model.DEFAULT) + "}}";
    withRewrittenComment(
        fixture,
        comment -> edit.accept((ObjectNode) comment.at("/collection/options/" + option)),
        VECTORIZE_SORT,
        response -> {
          assertApiError(response, code, snippet);
          assertApiError(postToFixture(fixture, request(VECTORIZE_SORT, override)), code, snippet);
        });
  }

  static Stream<Arguments> brokenSetting() {
    String iae = "IllegalArgumentException\nError Message: ";
    String rerank = iae + "Invalid reranking configuration for collection '%s.%s'";
    String lexical =
        iae + "Analyzer definition should be omitted, JSON null, or an empty JSON object {} if";
    String npe = "NullPointerException\nError Message: (null)";
    return Stream.of(
        broken("rerank on, no service", COMMENT, "rerank", s -> s.remove("service"), rerank),
        broken("rerank off, service kept", COMMENT, "rerank", s -> s.put("enabled", false), rerank),
        broken(
            "lexical on, no analyzer",
            COMMENT,
            "lexical",
            s -> s.removeAll().put("enabled", true),
            npe),
        broken(
            "lexical off, analyzer 'standard'",
            NOLEX,
            "lexical",
            s -> s.put("analyzer", "standard"),
            lexical),
        broken(
            "lexical off, analyzer object",
            NOLEX,
            "lexical",
            s -> s.putObject("analyzer").putObject("tokenizer").put("name", "standard"),
            lexical));
  }

  // On HCD (DSE has lexical off already), rewrites frr_comment's stored lexical setting to off with
  // a null or empty analyzer, which the API never writes. Expects plain off: an explicit $lexical
  // fails and a $vectorize sort still reranks. The docs do not cover stored settings; the test pins
  // current behavior so that any change is visible in review.
  @ParameterizedTest(name = "{0}")
  @MethodSource(CASES + "emptyAnalyzerWhileOff")
  @DisabledIfSystemProperty(named = TEST_PROP_LEXICAL_DISABLED, matches = "true")
  default void lexicalOffWithEmptyAnalyzerInTableCommentCountsAsOff(Consumer<ObjectNode> analyzer) {
    var code = SchemaException.Code.LEXICAL_NOT_ENABLED_FOR_COLLECTION;
    String snippet = "The collection without a lexical index: " + keyspace() + ".";
    withRewrittenComment(
        COMMENT,
        comment -> {
          var lexical = (ObjectNode) comment.at("/collection/options/lexical");
          analyzer.accept(lexical.put("enabled", false));
        },
        VECTORIZE_AND_LEXICAL,
        response -> {
          assertApiError(response, code, snippet + COMMENT.name());
          assertIds(postToFixture(COMMENT, request(VECTORIZE_SORT, null)), "k2", "k1");
          String[] texts = passages(COMMENT, "$vectorize", "k1", "k2");
          assertRerankerCalls(calls(1).query("ChatGPT upgraded").passages(texts));
        });
  }

  static Stream<Named<Consumer<ObjectNode>>> emptyAnalyzerWhileOff() {
    return Stream.of(
        Named.<Consumer<ObjectNode>>of("analyzer null", s -> s.putNull("analyzer")),
        Named.<Consumer<ObjectNode>>of("analyzer {}", s -> s.putObject("analyzer")));
  }

  // On HCD, removes the lexical and rerank settings from the schema_version 2 table comment of the
  // collection without lexical. Both then count as on: no override works (default model), and
  // $lexical makes the database reject the BM25 read. No document covers this. The test pins
  // current behavior; whether it is a bug is still under discussion.
  @Test
  @DisabledIfSystemProperty(named = TEST_PROP_LEXICAL_DISABLED, matches = "true")
  default void commentWithoutLexicalAndRerankSettingsUsesTheDefaults() {
    var code = DatabaseException.Code.INVALID_DATABASE_QUERY;
    withRewrittenComment(
        NOLEX,
        comment -> ((ObjectNode) comment.at("/collection/options")).remove(LEXICAL_AND_RERANK),
        VECTORIZE_AND_LEXICAL,
        response -> {
          assertApiError(response, code, "Undefined column name query_lexical_value");
          assertIds(postToFixture(NOLEX, request(VECTORIZE_SORT, null)), "n2", "n1");
          assertRerankerCalls(calls(1).query("ChatGPT upgraded").passages(SNEAKERS, NEW_DATA));
        });
  }

  // ---- helpers ----

  /** Rewrites the table comment, runs the checks in the new state, restores the old state. */
  private void withRewrittenComment(
      Fixture fixture,
      Consumer<ObjectNode> edit,
      String sort,
      Consumer<ValidatableResponse> checks) {
    String collection = ensure(fixture);
    String probe = request(sort, null);
    TableCommentRewriter.withRewrittenComment(
        keyspace(),
        collection,
        edit,
        () -> postToCollection(collection, probe),
        TableCommentRewriter::errorCode,
        response -> {
          FakeRerankerClient.reset();
          checks.accept(response);
        });
  }

  /** Searches one main-collection document by title with a $hybrid word; rank 1 in both reads. */
  private void assertFoundByBothReads(
      String id, String title, String word, float rerank, float vector) {
    FakeRerankerClient.reset();
    String filter = "{'title': '" + title + "'}";
    String request = request(filter, "{'$hybrid': '" + word + "'}", "{'includeScores': true}");
    var response = postToFixture(MAIN, request);
    assertIds(response, id);
    assertScores(response, scores(rerank, vector, 1, 1, rrf(1, 1)));
    assertRerankerCalls(calls(1).query(word).passages(passages(MAIN, "$vectorize", id)));
  }

  private static String service(String fields) {
    return "{'enabled': true, 'service': {" + fields + "}}";
  }

  private static String model(Model model) {
    return "'provider': '%s', 'modelName': '%s'".formatted(model.provider(), model.modelName());
  }

  /** An INVALID_CREATE_COLLECTION_OPTIONS case for these rerank options. */
  private static Arguments invalidRerank(String description, String rerank, String message) {
    var code = SchemaException.Code.INVALID_CREATE_COLLECTION_OPTIONS;
    String snippet = "'createCollection' command option(s) invalid: " + message;
    return args(description, rerank, code, snippet);
  }

  private static Arguments invalidService(String description, String fields, String message) {
    return invalidRerank(description, service(fields), message);
  }

  private static Arguments noVector(String description, String hybrid) {
    var code = SortException.Code.UNSUPPORTED_VECTOR_SORT_FOR_COLLECTION;
    return args(description, hybrid, NOVECTOR, code, "does not have vectors enabled");
  }

  private static Arguments noVectorize(String description, Fixture fixture, String hybrid) {
    var code = SortException.Code.UNSUPPORTED_VECTORIZE_SORT_FOR_COLLECTION;
    return args(description, hybrid, fixture, code, "does not have vectorize enabled");
  }

  private static Arguments conflict(String description, String fields) {
    var code = RequestException.Code.HYBRID_FIELD_CONFLICT;
    String snippet = "Field '$hybrid' cannot be used with '$lexical', '$vector', or";
    return args(description, insertOne(fields), MAIN, code, snippet);
  }

  /** A case that edits the rerank or lexical object of the table comment's options. */
  private static Arguments broken(
      String description, Fixture fixture, String option, Consumer<ObjectNode> edit, String error) {
    return args(description, fixture, option, edit, error);
  }

  /** Creates the temporary analyzer collection, returns its stored lexical setting, drops it. */
  private JsonNode storedLexical(String lexical) {
    String collection = TMP_ANALYZER.name();
    try {
      postToKeyspace(createCommand(collection, "{'lexical': " + lexical + "}"))
          .statusCode(200)
          .body("errors", is(nullValue()))
          .body("status.ok", is(1));
      assertRerankerNotCalled();
      var comment = parse(TableCommentRewriter.readComment(keyspace(), collection));
      return comment.at("/collection/options/lexical");
    } finally {
      postToKeyspace(json("{'deleteCollection': {'name': '" + collection + "'}}")).statusCode(200);
    }
  }

  /** The rejected writes stored no document with the _id "ok" or "bad" they use. */
  private void assertNothingStored(Fixture fixture) {
    String find = "{'find': {'filter': {'_id': {'$in': ['ok', 'bad']}}}}";
    postToFixture(fixture, json(find)).statusCode(200).body("data.documents", empty());
  }

  private static String withAnalyzer(String analyzer) {
    return "{'enabled': true, 'analyzer': " + analyzer + "}";
  }

  /** An INVALID_CREATE_COLLECTION_OPTIONS case for an analyzer of this JSON type. */
  private static Arguments wrongAnalyzer(String description, String analyzer, String type) {
    var code = SchemaException.Code.INVALID_CREATE_COLLECTION_OPTIONS;
    String must = "'analyzer' property of 'lexical' must be either JSON Object or String, is: ";
    return args(description, withAnalyzer(analyzer), code, new String[] {must + type});
  }

  private static String idsRequest(String group, String hybrid) {
    String options = "{'rerankQuery': 'kiwi', 'rerankOn': 'title', 'includeScores': true}";
    return request("{'grp': '" + group + "'}", "{'$hybrid': " + hybrid + "}", options);
  }

  private static String insertOne(String fields) {
    return json("{'insertOne': {'document': {'_id': 'bad', " + fields + "}}}");
  }

  /** An insertMany of a valid document "ok", then "bad" with these fields. */
  private static String insertTwo(String fields) {
    String documents =
        "[{'_id': 'ok', 'grp': 'tmp'}, {'_id': 'bad', 'grp': 'tmp', " + fields + "}]";
    return json("{'insertMany': {'documents': " + documents + "}}");
  }

  private static String createCommand(String options) {
    return createCommand("frr_tmp_invalid", options);
  }

  private static String createCommand(String name, String options) {
    return json("{'createCollection': {'name': '" + name + "', 'options': " + options + "}}");
  }
}
