package io.stargate.sgv2.jsonapi.api.v1.findandrerank;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsDDLSuccess;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankRequests.parse;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The collections and tables the findAndRerank tests use, with their documents. A {@link Fixture}
 * is created the first time a test asks for it ({@link FindAndRerankTestContext#ensure(Fixture)})
 * and lives until the class drops its keyspace; tests must not change the documents of a shared
 * fixture. DSE has no lexical, so a fixture can have a DSE definition, and on DSE {@link
 * Fixture#documents(boolean)} drops {@code $lexical} and turns {@code $hybrid} into {@code
 * $vectorize}. Every passage text must be in {@link FakeRerankerScores}, which also gives the
 * rerank orders. Inserts send at most 5 documents each, because the emulated HCD container times
 * out on larger writes.
 *
 * <p>Vector orders below are cosine values of the stored vectors; the {@code $vector} score of
 * {@code includeScores} is (1 + cosine) / 2, computed in float, so compare it within 1e-6. The test
 * embedding provider gives its six sample sentences their own vectors and every other text the same
 * vector, so documents with other texts tie in a vector read and tests must not assert their vector
 * ranks. A BM25 read on HCD only finds documents that contain every query term; the BM25 orders
 * follow from term counts in documents of equal length. Runs confirmed the vector orders on HCD and
 * DSE and the BM25 orders on HCD.
 */
public final class FindAndRerankFixtures {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** {@code "vector"} options for the test-only custom embedding provider, 5 dimensions. */
  public static final String CUSTOM_VECTORIZE_5 =
      """
      "vector": {
        "dimension": 5,
        "metric": "cosine",
        "service": {"provider": "custom", "modelName": "text-embedding-ada-002"}
      }""";

  /** The query vector of the official examples without $vectorize; {@link #BYO} matches it. */
  public static final String EXAMPLE_VECTOR = "[0.08, -0.62, 0.39]";

  /** The query vector for {@link #IDS}; its documents use [1,0,0], [1,1,0] or [1,3,0]. */
  public static final String IDS_VECTOR = "[1.0, 0.0, 0.0]";

  /** The BM25 query term for {@link #IDS}. */
  public static final String IDS_TERM = "kiwi";

  /**
   * A collection or table and its documents.
   *
   * @param name collection or table name
   * @param table true for a table; the definitions are then {@code createTable} definitions
   * @param hcdDefinition {@code createCollection} options (or the table definition) on HCD
   * @param dseDefinition the same for DSE
   * @param createHeaders extra headers for the create command and the inserts of the documents, for
   *     example a feature flag
   * @param documents document JSON texts, in insert order
   */
  public record Fixture(
      String name,
      boolean table,
      String hcdDefinition,
      String dseDefinition,
      Map<String, String> createHeaders,
      List<String> documents) {

    /** The {@code createCollection} or {@code createTable} command for this backend. */
    public String createCommand(boolean lexicalAvailable) {
      ObjectNode body = MAPPER.createObjectNode().put("name", name);
      body.set(
          table ? "definition" : "options",
          parse(lexicalAvailable ? hcdDefinition : dseDefinition));
      return MAPPER
          .createObjectNode()
          .set(table ? "createTable" : "createCollection", body)
          .toString();
    }

    /** The documents for this backend; see the class comment for the DSE changes. */
    public List<String> documents(boolean lexicalAvailable) {
      if (lexicalAvailable) {
        return documents;
      }
      return documents.stream()
          .map(
              text -> {
                ObjectNode doc = (ObjectNode) parse(text);
                doc.remove("$lexical");
                JsonNode hybrid = doc.remove("$hybrid");
                if (hybrid != null && (hybrid.isTextual() || hybrid.has("$vectorize"))) {
                  doc.set("$vectorize", hybrid.isTextual() ? hybrid : hybrid.get("$vectorize"));
                }
                return doc.toString();
              })
          .toList();
    }
  }

  /**
   * Collection without lexical, same on both backends: custom vectorize, rerank on with the default
   * model. For "ChatGPT upgraded" the vector order is n1, n2 and the rerank order n2, n1.
   */
  public static final Fixture NOLEX =
      collection(
          "frr_nolex",
          "{" + CUSTOM_VECTORIZE_5 + ", \"lexical\": {\"enabled\": false}}",
          """
          {"_id": "n1", "$vectorize": "ChatGPT integrated sneakers that talk to you"}""",
          """
          {"_id": "n2", "$vectorize": "New data updated"}""");

  /**
   * Collection whose table comment tests rewrite with CQL: custom vectorize, lexical and rerank
   * left to their defaults. The rerank order of its two documents is k2, k1.
   */
  public static final Fixture COMMENT =
      collection(
          "frr_comment",
          "{" + CUSTOM_VECTORIZE_5 + "}",
          """
          {"_id": "k1", "$vectorize": "An AI quilt to help you sleep forever"}""",
          """
          {"_id": "k2", "$vectorize": "A deep learning display that controls your mood"}""");

  /**
   * The main collection: custom vectorize, lexical (HCD only) and rerank left to their defaults.
   * Twelve documents have a {@code $vectorize} passage, more than the default limit and the batch
   * size (10).
   *
   * <ul>
   *   <li>m01 to m05, {@code grp: "distinct"}: five of the six sample sentences, so their vector
   *       ranks are fixed. Each {@code $lexical} has 14 words: house, hill, a, in, the, woods once,
   *       grassy and tree a varying number of times, meadow for the rest. Fields for the filter,
   *       projection and rerankOn tests: {@code is_checked_out} and {@code number_of_pages} (the
   *       filter example of the docs keeps m02, m03 and m05; m04 has exactly 300 pages), m01 {@code
   *       meta} object and {@code edge}, m02 {@code tags} array, m03 {@code a.b}, m04 {@code a} as
   *       an array of objects, m05 the dotted field name {@code "a.b"}.
   *   <li>m06 to m12, {@code grp: "plain"}: other texts, so they tie in a vector read. m06 is
   *       written with a {@code $hybrid} string and m07 with a {@code $hybrid} object; m08 has no
   *       {@code $lexical}; m09 to m12 have no title and an {@code edge} that is null, empty,
   *       whitespace and a no-break space. Their {@code $lexical} words appear nowhere else.
   *   <li>m13, {@code grp: "vec-only"}: only a {@code $vector}, equal to the vector of "ChatGPT
   *       upgraded", so it ranks first for that query but has no {@code $vectorize} passage.
   * </ul>
   *
   * <p>Orders of m01 to m05:
   *
   * <ul>
   *   <li>vector, "ChatGPT upgraded": m02 (0.9574), m04 (0.9537), m05 (0.8614), m03 (0.6450), m01
   *       (0.5331); m13 is 1.0 and the tied m06 to m12 are 0.9339.
   *   <li>vector, "A tree in the woods" (the shared vector): m04 (0.9442), m05 (0.8856), m02
   *       (0.8624), m01 (0.7502), m03 (0.7380); m06 to m12 are 1.0 and m13 is 0.9339.
   *   <li>BM25, "house hill grassy" (grassy 5, 4, 3, 2, 1 times): m03, m05, m01, m04, m02.
   *   <li>BM25, "A tree in the woods" (tree 5, 4, 3, 2, 1 times): m01, m02, m04, m03, m05.
   * </ul>
   */
  public static final Fixture MAIN =
      fixture(
          "frr_main",
          "{" + CUSTOM_VECTORIZE_5 + "}",
          "{" + CUSTOM_VECTORIZE_5 + "}",
          Map.of(),
          """
          {"_id": "m01", "grp": "distinct", "title": "Quilt for dreamers", "is_checked_out": true, "number_of_pages": 100, "meta": {"k": "v"}, "edge": "  Hello  ", "$vectorize": "An AI quilt to help you sleep forever", "$lexical": "house hill grassy grassy grassy a tree tree tree tree tree in the woods"}
          {"_id": "m02", "grp": "distinct", "title": "Talking sneakers", "is_checked_out": false, "number_of_pages": 150, "tags": ["x", "y"], "$vectorize": "ChatGPT integrated sneakers that talk to you", "$lexical": "house hill grassy a tree tree tree tree in the woods meadow meadow meadow"}
          {"_id": "m03", "grp": "distinct", "title": "Mood display", "is_checked_out": false, "number_of_pages": 290, "a": {"b": "x"}, "content": "Display panels change color with your mood", "$vectorize": "A deep learning display that controls your mood", "$lexical": "house hill grassy grassy grassy grassy grassy a tree tree in the woods meadow"}
          {"_id": "m04", "grp": "distinct", "title": "Data refresh", "is_checked_out": false, "number_of_pages": 300, "a": [{"b": "y"}], "$vectorize": "Updating new data", "$lexical": "house hill grassy grassy a tree tree tree in the woods meadow meadow meadow"}
          {"_id": "m05", "grp": "distinct", "title": "Fresh data", "is_checked_out": false, "number_of_pages": 120, "a.b": "z", "$vectorize": "New data updated", "$lexical": "house hill grassy grassy grassy grassy a tree in the woods meadow meadow meadow"}
          {"_id": "m06", "grp": "plain", "title": "Harbor lights", "n": 42, "content": "Boats rest in the harbor at night", "$hybrid": "Lanterns glow over the quiet harbor"}
          {"_id": "m07", "grp": "plain", "title": "Island charts", "d": 1.50, "content": "Sailors once used these charts", "$hybrid": {"$vectorize": "Old maps of forgotten islands", "$lexical": "maps islands sailors"}}
          {"_id": "m08", "grp": "plain", "title": "Pumpkin soup", "flag": true, "$vectorize": "A recipe for spiced pumpkin soup"}
          {"_id": "m09", "grp": "plain", "edge": null, "$vectorize": "Training notes for a marathon runner", "$lexical": "marathon runner training"}
          {"_id": "m10", "grp": "plain", "edge": "", "$vectorize": "Repairing a vintage bicycle chain", "$lexical": "bicycle chain repair"}
          {"_id": "m11", "grp": "plain", "edge": " \\t\\n ", "$vectorize": "Night sky photography for beginners", "$lexical": "night sky photography"}
          {"_id": "m12", "grp": "plain", "edge": "\\u00a0", "$vectorize": "Planting tomatoes on a city balcony", "$lexical": "tomatoes balcony garden"}
          {"_id": "m13", "grp": "vec-only", "$vector": [0.1, 0.16, 0.31, 0.22, 0.15]}
          """);

  /**
   * Collection with its own 3-dimension vectors, for the official examples that send {@link
   * #EXAMPLE_VECTOR}: no vectorize, lexical (HCD only) and rerank left to their defaults. No
   * document has a {@code content} field.
   *
   * <ul>
   *   <li>{@code grp: "order"}, o1 to o4: four orders that all differ. Vector o3, o1, o4, o2. BM25
   *       for "house hill grassy" o2, o4, o1, o3 (8 words each, grassy 2, 4, 1, 3 times). Rerank on
   *       {@code $lexical} or {@code title} o4, o3, o2, o1. {@code _id} o1, o2, o3, o4. The filter
   *       example of the docs keeps o1 and o3; {@code example_field} is a string, 7, true and
   *       missing, and reranking on it gives o2, o1, o3.
   *   <li>b5 has only {@code $lexical} (the three query words), b6 only a vector, b7 a null {@code
   *       $lexical}, b8 the boots sentence, b9 a long {@code $lexical} with each query word once.
   * </ul>
   *
   * <p>Vector order of all eight vectors: o3 (0.9992), b9 (0.9791), o1 (0.9071), b6 (0.8415), o4
   * (0.6079), b7 (0.5115), b8 (0.3338), o2 (0.1357). BM25 for "house hill grassy": b5, o2, o4, o1,
   * o3, b9.
   */
  public static final Fixture BYO =
      fixture(
          "frr_byo",
          "{\"vector\": {\"dimension\": 3, \"metric\": \"cosine\"}}",
          "{\"vector\": {\"dimension\": 3, \"metric\": \"cosine\"}}",
          Map.of(),
          """
          {"_id": "o1", "grp": "order", "title": "Hillside cottage", "is_checked_out": false, "number_of_pages": 120, "example_field": "Tall trees and quiet woods", "$vector": [0.3, -0.5, 0.2], "$lexical": "house hill grassy grassy meadow meadow meadow meadow"}
          {"_id": "o2", "grp": "order", "title": "Meadow farmhouse", "is_checked_out": true, "number_of_pages": 250, "example_field": 7, "$vector": [0.6, 0.1, 0.2], "$lexical": "house hill grassy grassy grassy grassy meadow meadow"}
          {"_id": "o3", "grp": "order", "title": "Valley cabin", "is_checked_out": false, "number_of_pages": 280, "example_field": true, "$vector": [0.1, -0.6, 0.4], "$lexical": "house hill grassy meadow meadow meadow meadow meadow"}
          {"_id": "o4", "grp": "order", "title": "Ridge lodge", "is_checked_out": false, "number_of_pages": 300, "$vector": [0.5, -0.3, 0.1], "$lexical": "house hill grassy grassy grassy meadow meadow meadow"}
          {"_id": "b5", "grp": "lex-only", "$lexical": "house hill grassy"}
          {"_id": "b6", "grp": "vec-only", "title": "Lone vector", "$vector": [0.4, -0.4, 0.3]}
          {"_id": "b7", "grp": "lex-null", "title": "Lonely pine", "$vector": [0.6, -0.2, 0.2], "$lexical": null}
          {"_id": "b8", "grp": "boots", "$vector": [0.6, 0.0, 0.3], "$lexical": "These boots were made for hiking, but they are not waterproof"}
          {"_id": "b9", "grp": "both", "$vector": [0.2, -0.6, 0.3], "$lexical": "a house on a hill with one grassy path and many old stone walls around it"}
          """);

  /**
   * Collection with rerank disabled: custom vectorize, lexical left to its default, no {@code
   * $lexical} in the documents. Vector order for "A tree in the woods": r3, r2, r1; for "ChatGPT
   * upgraded": r3, r1, r2. Rerank order (with an override): r1, r3, r2.
   */
  public static final Fixture NORERANK =
      collection(
          "frr_norerank",
          "{" + CUSTOM_VECTORIZE_5 + ", \"rerank\": {\"enabled\": false, \"service\": {}}}",
          """
          {"_id": "r1", "$vectorize": "A deep learning display that controls your mood"}""",
          """
          {"_id": "r2", "$vectorize": "An AI quilt to help you sleep forever"}""",
          """
          {"_id": "r3", "$vectorize": "Updating new data"}""");

  /** Collection without vector and without lexical, rerank on; no documents. */
  public static final Fixture NOVECTOR =
      collection("frr_novector", "{\"lexical\": {\"enabled\": false}}");

  /**
   * Collection with a 3-dimension vector but no vectorize service, without lexical, and with {@code
   * $vector} denied from indexing; rerank on; no documents.
   */
  public static final Fixture NOVECTORIZE =
      collection(
          "frr_novectorize",
          """
          {"vector": {"dimension": 3, "metric": "cosine"}, "lexical": {"enabled": false},
           "indexing": {"deny": ["$vector"]}}""");

  /**
   * Collection for ties and unusual {@code _id} values. Tests always filter on one group, because
   * reading the null or object {@code _id} fails the whole request. 3-dimension vectors without
   * vectorize, {@code rerank: {"enabled": true}} without a service, selective indexing of {@code
   * grp}, {@code pairs} and {@code title}. On HCD lexical uses an analyzer object with stop words
   * and Porter stemming; on DSE there is no lexical. Query with {@link #IDS_VECTOR} and {@link
   * #IDS_TERM}: [1,0,0] has cosine 1.0, [1,1,0] 0.7071, [1,3,0] 0.3162.
   *
   * <ul>
   *   <li>{@code pairs} "p-str" ("a", "b"), "p-prefix" ("q", "q!"), "p-num" (9, 10), "p-type1"
   *       (true, 5) and "p-type2" (5, "c"): the first document is the nearest vector and has no
   *       {@code $lexical}, the second is far and is the only BM25 match. With {@code hybridLimits}
   *       1 for both reads each is found by one read only at rank 1, so their RRF values are equal.
   *       Document 5 is in both type groups. "q" sorts before "q!" as plain text but after it as
   *       quoted JSON text.
   *   <li>{@code pairs} "p-rrf": vector Y, X, Z; BM25 Z, X ("kiwi kiwi" before "kiwi lemon"). X and
   *       Y share a title; Z has a higher-scored title.
   *   <li>{@code pairs} "p-vec": vb then va in the vector read, same title.
   *   <li>{@code grp} "dedup" (string "1" and number 1, different titles, 1 scores higher),
   *       "dup-passage" (d2 then d1 in the vector read, same title), "nullid", "objid", "nomatch"
   *       (only nm-hit contains the term; "cherry" matches both with equal BM25 scores and "kiwi
   *       durian" matches neither), "author" (a field that is not indexed), "analyzer" ("kiwis"
   *       matches the term only through stemming).
   * </ul>
   *
   * <p>All tie documents have the title "Same title", so they get the same rerank score.
   */
  public static final Fixture IDS =
      fixture(
          "frr_ids",
          """
          {"vector": {"dimension": 3, "metric": "cosine"},
           "lexical": {"enabled": true, "analyzer": {"tokenizer": {"name": "standard"},
             "filters": [{"name": "lowercase"}, {"name": "stop"}, {"name": "porterstem"},
                         {"name": "asciifolding"}]}},
           "rerank": {"enabled": true},
           "indexing": {"allow": ["grp", "pairs", "title"]}}""",
          """
          {"vector": {"dimension": 3, "metric": "cosine"},
           "rerank": {"enabled": true},
           "indexing": {"allow": ["grp", "pairs", "title"]}}""",
          Map.of(),
          """
          {"_id": "a", "pairs": "p-str", "title": "Same title", "$vector": [1.0, 0.0, 0.0]}
          {"_id": "b", "pairs": "p-str", "title": "Same title", "$vector": [1.0, 3.0, 0.0], "$lexical": "kiwi"}
          {"_id": "q", "pairs": "p-prefix", "title": "Same title", "$vector": [1.0, 0.0, 0.0]}
          {"_id": "q!", "pairs": "p-prefix", "title": "Same title", "$vector": [1.0, 3.0, 0.0], "$lexical": "kiwi"}
          {"_id": 9, "pairs": "p-num", "title": "Same title", "$vector": [1.0, 0.0, 0.0]}
          {"_id": 10, "pairs": "p-num", "title": "Same title", "$vector": [1.0, 3.0, 0.0], "$lexical": "kiwi"}
          {"_id": true, "pairs": "p-type1", "title": "Same title", "$vector": [1.0, 0.0, 0.0]}
          {"_id": 5, "pairs": ["p-type1", "p-type2"], "title": "Same title", "$vector": [1.0, 3.0, 0.0], "$lexical": "kiwi"}
          {"_id": "c", "pairs": "p-type2", "title": "Same title", "$vector": [1.0, 0.0, 0.0]}
          {"_id": "X", "pairs": "p-rrf", "title": "Same title", "$vector": [1.0, 1.0, 0.0], "$lexical": "kiwi lemon"}
          {"_id": "Y", "pairs": "p-rrf", "title": "Same title", "$vector": [1.0, 0.0, 0.0]}
          {"_id": "Z", "pairs": "p-rrf", "title": "Different title", "$vector": [1.0, 3.0, 0.0], "$lexical": "kiwi kiwi"}
          {"_id": "va", "pairs": "p-vec", "title": "Same title", "$vector": [1.0, 1.0, 0.0]}
          {"_id": "vb", "pairs": "p-vec", "title": "Same title", "$vector": [1.0, 0.0, 0.0]}
          {"_id": "1", "grp": "dedup", "title": "String one", "$vector": [1.0, 1.0, 0.0]}
          {"_id": 1, "grp": "dedup", "title": "Number one", "$vector": [1.0, 0.0, 0.0]}
          {"_id": "d1", "grp": "dup-passage", "title": "Same title", "$vector": [1.0, 1.0, 0.0]}
          {"_id": "d2", "grp": "dup-passage", "title": "Same title", "$vector": [1.0, 0.0, 0.0]}
          {"_id": null, "grp": "nullid", "title": "Null id", "$vector": [1.0, 0.0, 0.0]}
          {"_id": {"$uuid": "4c5a1e2e-8f2b-4a0e-9b1a-3f2d6c7e8a90"}, "grp": "objid", "title": "Object id", "$vector": [1.0, 0.0, 0.0]}
          {"_id": "nm-hit", "grp": "nomatch", "title": "Has the term", "$vector": [1.0, 0.0, 0.0], "$lexical": "kiwi cherry"}
          {"_id": "nm-miss", "grp": "nomatch", "title": "Lacks the term", "$vector": [1.0, 1.0, 0.0], "$lexical": "cherry durian"}
          {"_id": "au1", "grp": "author", "author": "Ann", "title": "Has author", "$vector": [1.0, 0.0, 0.0]}
          {"_id": "an1", "grp": "analyzer", "title": "Stemmed words", "$vector": [1.0, 0.0, 0.0], "$lexical": "The kiwis were ripening"}
          """);

  /** Table definition with a 5-dimension vector column that has the custom vectorize service. */
  private static final String TABLE_DEFINITION =
      """
      {"columns": {
         "id": {"type": "text"},
         "name": {"type": "text"},
         "vec": {"type": "vector", "dimension": 5,
                 "service": {"provider": "custom", "modelName": "text-embedding-ada-002"}}},
       "primaryKey": "id"}""";

  /** Table for the findAndRerank-on-a-table tests; no rows. */
  public static final Fixture TABLE = table("frr_table");

  /**
   * Collection of the feature-off class: custom vectorize, lexical and rerank left to their
   * defaults, created with the reranking feature header so that rerank is stored as enabled. Vector
   * order for "ChatGPT upgraded": f1, f2; rerank on {@code $vectorize}: f2, f1. On HCD, BM25 for
   * "orchard" finds f1 then f2, and rerank on {@code $lexical} gives f1, f2.
   */
  public static final Fixture OFF_RERANK =
      fixture(
          "frr_off_rerank",
          "{" + CUSTOM_VECTORIZE_5 + "}",
          "{" + CUSTOM_VECTORIZE_5 + "}",
          Map.of(ApiFeature.RERANKING.httpHeaderName(), "true"),
          """
          {"_id": "f1", "$vectorize": "Updating new data", "$lexical": "orchard orchard apple"}
          {"_id": "f2", "$vectorize": "A deep learning display that controls your mood", "$lexical": "orchard pear harvest"}
          """);

  /**
   * Collection of the feature-off class with rerank disabled; no documents. It is created with the
   * reranking feature header, like {@link #OFF_RERANK}, so that the create command does not depend
   * on the feature flag.
   */
  public static final Fixture OFF_NORERANK =
      fixture(
          "frr_off_norerank",
          "{" + CUSTOM_VECTORIZE_5 + ", \"rerank\": {\"enabled\": false}}",
          "{" + CUSTOM_VECTORIZE_5 + ", \"rerank\": {\"enabled\": false}}",
          Map.of(ApiFeature.RERANKING.httpHeaderName(), "true"),
          "");

  /** Table of the feature-off class, same definition as {@link #TABLE}; no rows. */
  public static final Fixture OFF_TABLE = table("frr_off_table");

  /**
   * Name of the temporary collection that successful createCollection cases create with their own
   * options and drop in {@code finally}: lexical on (HCD only), lexical off (both backends), or
   * lexical off with a rerank service (both backends). They use only its name and never call {@code
   * ensure}, so it has no options here.
   */
  public static final Fixture TMP_ANALYZER = collection("frr_tmp_analyzer", "{}");

  /**
   * Created and dropped by the single test that uses the custom embedding provider with a dimension
   * it has no vectors for. HCD and DSE accept the create command.
   */
  public static final Fixture TMP_DIM4 =
      collection(
          "frr_tmp_dim4",
          """
          {"vector": {
             "dimension": 4,
             "metric": "cosine",
             "service": {"provider": "custom", "modelName": "text-embedding-ada-002"}}}""");

  private FindAndRerankFixtures() {}

  /** A collection with the same definition on both backends and no extra create headers. */
  public static Fixture collection(String name, String options, String... documents) {
    return new Fixture(name, false, options, options, Map.of(), List.of(documents));
  }

  /** A collection whose documents are given one JSON object per line, kept exactly as written. */
  private static Fixture fixture(
      String name,
      String hcdOptions,
      String dseOptions,
      Map<String, String> createHeaders,
      String documentLines) {
    List<String> documents =
        documentLines.lines().map(String::strip).filter(line -> !line.isEmpty()).toList();
    return new Fixture(name, false, hcdOptions, dseOptions, createHeaders, documents);
  }

  private static Fixture table(String name) {
    return new Fixture(name, true, TABLE_DEFINITION, TABLE_DEFINITION, Map.of(), List.of());
  }

  /**
   * The expected passages: the text of a top-level {@code field} in these documents, in this order,
   * as the reranker receives it. A number or boolean becomes its text; {@code $vectorize} and
   * {@code $lexical} also come from a {@code $hybrid} string or object. Each id, compared as text,
   * must match exactly one document.
   */
  public static String[] passages(Fixture fixture, String field, String... ids) {
    List<JsonNode> docs = fixture.documents().stream().map(FindAndRerankRequests::parse).toList();
    return Arrays.stream(ids)
        .map(
            id -> {
              List<JsonNode> found =
                  docs.stream().filter(d -> d.path("_id").asText().equals(id)).toList();
              if (found.size() != 1) {
                throw new IllegalArgumentException(
                    "%d documents with _id %s in %s".formatted(found.size(), id, fixture.name()));
              }
              JsonNode value = found.get(0).get(field);
              JsonNode hybrid = found.get(0).get("$hybrid");
              if (value == null && hybrid != null && field.startsWith("$")) {
                value = hybrid.isTextual() ? hybrid : hybrid.get(field);
              }
              if (value == null || !value.isValueNode() || value.isNull()) {
                throw new IllegalArgumentException(
                    "%s of %s in %s is not text, a number or a boolean: %s"
                        .formatted(field, id, fixture.name(), value));
              }
              return value.asText();
            })
        .toArray(String[]::new);
  }

  /** Creates the fixture and inserts its documents unless {@code created} has its name. */
  public static String ensure(FindAndRerankTestContext ctx, Fixture fixture, Set<String> created) {
    synchronized (created) {
      if (!created.contains(fixture.name())) {
        Map<String, Object> headers = new LinkedHashMap<>(ctx.defaultHeaders());
        headers.putAll(fixture.createHeaders());
        ctx.postToKeyspace(fixture.createCommand(ctx.lexicalAvailable()), headers)
            .statusCode(200)
            .body("$", responseIsDDLSuccess())
            .body("status.ok", is(1));
        List<String> docs = fixture.documents(ctx.lexicalAvailable());
        for (int from = 0; from < docs.size(); from += 5) {
          List<String> chunk = docs.subList(from, Math.min(from + 5, docs.size()));
          ctx.postToCollection(
                  fixture.name(),
                  "{\"insertMany\": {\"documents\": [" + String.join(",", chunk) + "]}}",
                  headers)
              .statusCode(200)
              .body("errors", is(nullValue()))
              .body("status.insertedIds", hasSize(chunk.size()));
        }
        created.add(fixture.name());
      }
      return fixture.name();
    }
  }
}
