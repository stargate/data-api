package io.stargate.sgv2.jsonapi.api.v1.findandrerank;

import static java.util.Map.entry;

import java.util.Map;

/**
 * The fixed score ({@code logit}) the fake reranker gives each passage text in normal mode.
 *
 * <p>A passage missing from this table, unless it starts with {@code score:<number>}, makes the
 * request "unrecognized" (HTTP 500 and a failing {@code @AfterEach} check), so a typo in a fixture
 * cannot silently get a score. The table lists every field value of {@link FindAndRerankFixtures}
 * that a test may use as a passage, grouped by collection and field; each group comment gives the
 * resulting order. Scores are multiples of 1/8, so they are exact as float. Within one field of one
 * collection they all differ, and their order differs from the {@code _id}, vector and BM25 orders,
 * so a response order proves that the reranker decided it. The one exception is "Same title" in
 * {@code frr_ids}, shared on purpose to create ties.
 */
public final class FakeRerankerScores {

  private static final Map<String, Float> SCORES =
      Map.ofEntries(
          // The six sentences that CustomITEmbeddingProvider gives distinct vectors to. Shared by
          // every collection that uses the custom vectorize service.
          entry("ChatGPT integrated sneakers that talk to you", 1.5f),
          entry("ChatGPT upgraded", 4.0f),
          entry("New data updated", 3.25f),
          entry("An AI quilt to help you sleep forever", -0.75f),
          entry("A deep learning display that controls your mood", 2.125f),
          entry("Updating new data", 0.5f),
          // frr_main $vectorize of m06 to m12 (m06's text is also its $lexical). With the five
          // sentences of m01 to m05 the order is m08 m05 m11 m03 m06 m02 m12 m04 m09 m01 m07 m10.
          entry("Lanterns glow over the quiet harbor", 1.875f),
          entry("Old maps of forgotten islands", -1.25f),
          entry("A recipe for spiced pumpkin soup", 3.75f),
          entry("Training notes for a marathon runner", 0.25f),
          entry("Repairing a vintage bicycle chain", -2.5f),
          entry("Night sky photography for beginners", 2.75f),
          entry("Planting tomatoes on a city balcony", 1.0f),
          // frr_main $lexical (HCD only), m01 to m05, m07, m09 to m12. With m06 the order is
          // m07 m12 m04 m06 m01 m10 m05 m02 m09 m03 m11.
          entry("house hill grassy grassy grassy a tree tree tree tree tree in the woods", 1.25f),
          entry(
              "house hill grassy a tree tree tree tree in the woods meadow meadow meadow", -0.375f),
          entry(
              "house hill grassy grassy grassy grassy grassy a tree tree in the woods meadow",
              -1.75f),
          entry(
              "house hill grassy grassy a tree tree tree in the woods meadow meadow meadow",
              2.375f),
          entry(
              "house hill grassy grassy grassy grassy a tree in the woods meadow meadow meadow",
              0.375f),
          entry("maps islands sailors", 3.0f),
          entry("marathon runner training", -1.0f),
          entry("bicycle chain repair", 0.625f),
          entry("night sky photography", -2.0f),
          entry("tomatoes balcony garden", 2.625f),
          // frr_main title, m01 to m08: m06 m01 m08 m03 m05 m02 m07 m04.
          entry("Quilt for dreamers", 2.25f),
          entry("Talking sneakers", -0.5f),
          entry("Mood display", 1.125f),
          entry("Data refresh", -1.625f),
          entry("Fresh data", 0.875f),
          entry("Harbor lights", 3.5f),
          entry("Island charts", -1.125f),
          entry("Pumpkin soup", 1.625f),
          // frr_main content (m06 m03 m07) and edge (m01 m12; the other edge values are dropped).
          entry("Display panels change color with your mood", 0.125f),
          entry("Boats rest in the harbor at night", 2.875f),
          entry("Sailors once used these charts", -0.625f),
          entry("  Hello  ", 1.375f),
          entry(" ", -0.875f),
          // Text forms of non-string values: frr_main n (m06), d (m07, stored as 1.50), flag
          // (m08), a.b (m03) and tags.0 (m02) give "x", "a&.b" (m05) gives "z"; frr_byo
          // example_field gives "7" (o2) and "true" (o3).
          entry("42", 0.625f),
          entry("1.5", 0.75f),
          entry("true", -1.5f),
          entry("x", 0.25f),
          entry("z", 1.0f),
          entry("7", 2.625f),
          // frr_main _id: m06 m10 m02 m08 m04 m13 m12 m01 m07 m05 m09 m03 m11.
          entry("m01", 0.5f),
          entry("m02", 2.0f),
          entry("m03", -1.0f),
          entry("m04", 1.25f),
          entry("m05", -0.25f),
          entry("m06", 3.0f),
          entry("m07", 0.125f),
          entry("m08", 1.75f),
          entry("m09", -0.5f),
          entry("m10", 2.5f),
          entry("m11", -1.25f),
          entry("m12", 0.75f),
          entry("m13", 1.0f),
          // frr_byo $lexical (HCD only): b5 o4 o3 b9 o2 b8 o1; within o1 to o4: o4 o3 o2 o1.
          entry("house hill grassy grassy meadow meadow meadow meadow", -0.5f),
          entry("house hill grassy grassy grassy grassy meadow meadow", 1.0f),
          entry("house hill grassy meadow meadow meadow meadow meadow", 2.25f),
          entry("house hill grassy grassy grassy meadow meadow meadow", 3.5f),
          entry("house hill grassy", 4.25f),
          entry("These boots were made for hiking, but they are not waterproof", 0.125f),
          entry("a house on a hill with one grassy path and many old stone walls around it", 1.75f),
          // frr_byo title: o4 o3 b7 o2 b6 o1. example_field: o2 ("7") o1 o3 ("true").
          entry("Hillside cottage", -0.25f),
          entry("Meadow farmhouse", 0.75f),
          entry("Valley cabin", 2.0f),
          entry("Ridge lodge", 3.0f),
          entry("Lone vector", 0.25f),
          entry("Lonely pine", 1.5f),
          entry("Tall trees and quiet woods", 1.25f),
          // frr_ids title. All tie documents share "Same title"; "Different title" (Z) is higher.
          // dedup: 1 before "1". The other groups have one title each.
          entry("Same title", 1.0f),
          entry("Different title", 2.5f),
          entry("String one", 0.25f),
          entry("Number one", 1.75f),
          entry("Null id", -0.25f),
          entry("Object id", -0.5f),
          entry("Has the term", 0.875f),
          entry("Lacks the term", 1.375f),
          entry("Has author", 0.625f),
          entry("Stemmed words", 1.125f),
          // frr_ids $lexical (HCD only).
          entry("kiwi", 0.5f),
          entry("kiwi lemon", -1.0f),
          entry("kiwi kiwi", 2.0f),
          entry("kiwi cherry", 1.5f),
          entry("cherry durian", -0.75f),
          entry("The kiwis were ripening", 0.25f),
          // frr_off_rerank $lexical (HCD only): f1 f2.
          entry("orchard orchard apple", 1.25f),
          entry("orchard pear harvest", -0.25f));

  private FakeRerankerScores() {}

  /** Returns the score of this passage, or null when the passage is not in the table. */
  public static Float find(String passage) {
    return SCORES.get(passage);
  }

  /** The {@code HASH_SCORES} score: {@code (hashCode % 1000) / 8}, from -124.875 to 124.875. */
  public static float hashScore(String passage) {
    return (passage.hashCode() % 1000) / 8f;
  }
}
