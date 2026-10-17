package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

import static io.stargate.sgv2.jsonapi.util.JsonUtil.arrayNodeToVector;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.EJSONWrapper;
import io.stargate.sgv2.jsonapi.exception.SortException;
import io.stargate.sgv2.jsonapi.metrics.CommandFeatures;
import io.stargate.sgv2.jsonapi.util.JsonFieldMatcher;
import java.io.IOException;
import java.util.*;

/**
 * {@link StdDeserializer} for the {@link FindAndRerankSort}.
 *
 * <p>Checks the JSON structure and value types. A nonempty sort must have exactly one top-level
 * field to select its mode. The planner validates the values against the collection and decides
 * which legs run. For example, the text of <code>{"$hybrid": "text"}</code> is kept as hybrid text
 * for the planner to use.
 */
public class FindAndRerankSortClauseDeserializer extends StdDeserializer<FindAndRerankSort> {

  private static final String ERROR_CONTEXT = "`sort` clause";

  // top level has exactly one of $hybrid, $vectorize, $vector or $lexical
  // can be string or object for $hybrid
  // { "sort" : { "$hybrid" : "i like cheese" } }
  // { "sort" : { "$hybrid" : { "$vectorize" : "i like cheese", "$lexical" : "cows"} } }
  // { "sort" : { "$hybrid" : { "$vector" : [1, 2, 3], "$lexical" : "cows"} } }
  // { "sort" : { "$vectorize" : "i like cheese" } }
  // { "sort" : { "$vector" : [1, 2, 3] } }
  // { "sort" : { "$lexical" : "cows" } }
  private static final JsonFieldMatcher<JsonNode> MATCH_TOP_LEVEL_SORT_FIELDS =
      new JsonFieldMatcher<>(
          JsonNode.class,
          List.of(),
          List.of(
              LegMode.HYBRID.sortField(),
              LegMode.VECTORIZE.sortField(),
              LegMode.VECTOR.sortField(),
              LegMode.LEXICAL.sortField()));

  // user is specifying the sorts individually inside the $hybrid object
  // can be
  // { "sort" : { "$hybrid" : { "$vectorize" : "i like cheese", "$lexical" : "cows"} } }
  // { "sort" : { "$hybrid" : { "$vector" : [1, 2, 3], "$lexical" : "cows"} } }
  // they could also set something to null to skip that read, that is handled by the resolver
  // later.
  private static final JsonFieldMatcher<JsonNode> MATCH_HYBRID_OBJECT_FIELDS =
      new JsonFieldMatcher<>(
          JsonNode.class,
          List.of(),
          List.of(
              LegMode.LEXICAL.sortField(),
              LegMode.VECTORIZE.sortField(),
              LegMode.VECTOR.sortField()));

  public FindAndRerankSortClauseDeserializer() {
    super(FindAndRerankSort.class);
  }

  /** {@inheritDoc} */
  @Override
  public FindAndRerankSort deserialize(
      JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {

    return switch (deserializationContext.readTree(jsonParser)) {
      case NullNode ignored -> // this is {"sort" : null}
          FindAndRerankSort.NO_ARG_SORT;
      case ObjectNode objectNode -> deserialize(jsonParser, objectNode);
      default ->
          throw new JsonMappingException(
              jsonParser, "sort clause must be an object or null", jsonParser.currentLocation());
    };
  }

  /**
   * Deserialize the sort clause from an object node.
   *
   * @param sort The sort clause as an object node, no check for nulls.
   * @return The {@link FindAndRerankSort} after checking its JSON structure and value types. Inside
   *     <code>$hybrid</code>, both <code>$vectorize</code> and <code>$vector</code> are kept for
   *     the planner to validate. A single-leg sort with no value, e.g. <code>
   *     {"$lexical": null}</code>, is recorded as that mode with no value, the planner rejects it
   *     (for now every single-leg mode fails with UNSUPPORTED_FIND_AND_RERANK_SORT, GH #2602).
   */
  private static FindAndRerankSort deserialize(JsonParser jsonParser, ObjectNode sort)
      throws JsonMappingException {

    // { "sort" : { } }
    if (sort.isEmpty()) {
      return FindAndRerankSort.NO_ARG_SORT;
    }

    var sortMatch = MATCH_TOP_LEVEL_SORT_FIELDS.matchAndThrow(sort, jsonParser, ERROR_CONTEXT);
    if (sortMatch.matched().size() != 1) {
      throw SortException.Code.SORT_CLAUSE_INVALID.get(
          Map.of(
              "problem",
              "sort clause must have exactly one of $hybrid, $vectorize, $vector, $lexical, but got: "
                  + String.join(", ", sortMatch.matched().keySet())));
    }

    // the matcher has checked the field name, so fromSortField cannot fail here
    var topLevelField = sortMatch.matched().entrySet().iterator().next();
    var legMode = LegMode.fromSortField(topLevelField.getKey());
    var topLevelValue = topLevelField.getValue();

    return switch (legMode) {
      case HYBRID ->
          switch (topLevelValue) {
            case TextNode textNode ->
                FindAndRerankSort.fromHybridText(
                    normalizedText(textNode.asText()),
                    CommandFeatures.of(LegMode.HYBRID.commandFeature()));
            case ObjectNode objectNode -> deserializeHybridObject(jsonParser, objectNode);
            case JsonNode node ->
                throw JsonFieldMatcher.errorForWrongType(
                    jsonParser,
                    ERROR_CONTEXT,
                    LegMode.HYBRID.sortField(),
                    node,
                    TextNode.class,
                    ObjectNode.class);
          };
      case VECTORIZE -> {
        var commandFeatures = CommandFeatures.create();
        yield FindAndRerankSort.fromVectorizeSort(
            textSortValue(jsonParser, legMode, topLevelValue, commandFeatures), commandFeatures);
      }
      case LEXICAL -> {
        var commandFeatures = CommandFeatures.create();
        yield FindAndRerankSort.fromLexicalSort(
            textSortValue(jsonParser, legMode, topLevelValue, commandFeatures), commandFeatures);
      }
      case VECTOR -> {
        var commandFeatures = CommandFeatures.create();
        yield FindAndRerankSort.fromVectorSort(
            vectorSortValue(jsonParser, topLevelValue, commandFeatures), commandFeatures);
      }
      case FILTER ->
          throw new IllegalStateException("FILTER has no sort field: " + topLevelField.getKey());
    };
  }

  private static FindAndRerankSort deserializeHybridObject(
      JsonParser jsonParser, ObjectNode hybridObject) throws JsonMappingException {

    var sortMatch =
        MATCH_HYBRID_OBJECT_FIELDS.matchAndThrow(hybridObject, jsonParser, ERROR_CONTEXT);
    // NOTE: commandFeatures are only for metrics, the FindAndRerankPlanner decides what to run
    // from the legMode and the sort values
    CommandFeatures commandFeatures = CommandFeatures.of(LegMode.HYBRID.commandFeature());

    var vectorizeText =
        textSortValue(
            jsonParser,
            LegMode.VECTORIZE,
            sortMatch.matched().get(LegMode.VECTORIZE.sortField()),
            commandFeatures);
    var lexicalText =
        textSortValue(
            jsonParser,
            LegMode.LEXICAL,
            sortMatch.matched().get(LegMode.LEXICAL.sortField()),
            commandFeatures);
    var vector =
        vectorSortValue(
            jsonParser, sortMatch.matched().get(LegMode.VECTOR.sortField()), commandFeatures);

    return FindAndRerankSort.fromHybridObject(vectorizeText, lexicalText, vector, commandFeatures);
  }

  /**
   * A text sort value for {@code legMode}'s field, from inside the {@code $hybrid} object or from a
   * top-level single-leg sort. The field and its metrics feature both come from {@code legMode}, so
   * they cannot mismatch.
   *
   * @param legMode {@link LegMode#VECTORIZE} or {@link LegMode#LEXICAL}.
   * @param node The field value, or {@code null} when the field is missing.
   * @param commandFeatures The mode's feature is added when the field was present, even if null.
   * @return The trimmed text, or {@code null} when the field is missing, JSON null or blank. Inside
   *     {@code $hybrid} that skips the leg, at the top level it is a single-leg sort with no value,
   *     the planner rejects it.
   */
  private static String textSortValue(
      JsonParser jsonParser, LegMode legMode, JsonNode node, CommandFeatures commandFeatures)
      throws JsonMappingException {
    return switch (node) {
      case null -> {
        // undefined in the object, up to the resolver to check if valid
        // { "sort" : { "$hybrid" : { }}
        yield null;
      }
      case NullNode ignored -> {
        // explicit setting to null is allowed
        // { "sort" : { "$hybrid" : { "$vectorize" : null } } } or { "sort" : { "$lexical" : null }
        // }
        commandFeatures.addFeature(legMode.commandFeature());
        yield null;
      }
      case TextNode textNode -> {
        // { "sort" : { "$hybrid" : { "$vectorize" : "I like cheese" } } } or
        // { "sort" : { "$lexical" : "cheese" } }
        commandFeatures.addFeature(legMode.commandFeature());
        yield normalizedText(textNode.asText());
      }
      case JsonNode wrongType ->
          throw JsonFieldMatcher.errorForWrongType(
              jsonParser,
              ERROR_CONTEXT,
              legMode.sortField(),
              wrongType,
              NullNode.class,
              TextNode.class);
    };
  }

  /**
   * A {@code $vector} sort value, from inside the {@code $hybrid} object or from a top-level
   * single-leg sort.
   *
   * @param node The field value, or {@code null} when the field is missing.
   * @param commandFeatures The vector feature is added when the field was present, even if null.
   * @return The vector as written, or {@code null} when the field is missing or JSON null. An empty
   *     array fails in {@code arrayNodeToVector} with SHRED_BAD_VECTOR_SIZE.
   */
  private static float[] vectorSortValue(
      JsonParser jsonParser, JsonNode node, CommandFeatures commandFeatures)
      throws JsonMappingException {
    return switch (node) {
      case null -> {
        // undefined in the object, up to the resolver to check if valid
        // { "sort" : { "$hybrid" : { }}
        yield null;
      }
      case NullNode ignored -> {
        // explicit setting to null is allowed
        // { "sort" : { "$hybrid" : { "$vector" : null } } } or { "sort" : { "$vector" : null } }
        commandFeatures.addFeature(LegMode.VECTOR.commandFeature());
        yield null;
      }
      case ArrayNode arrayNode -> {
        // { "sort" : { "$hybrid" : { "$vector" : [1,2,3] } } } or { "sort" : { "$vector" : [] } }
        commandFeatures.addFeature(LegMode.VECTOR.commandFeature());
        yield arrayNodeToVector(arrayNode);
      }
      case ObjectNode objectNode -> {
        // binary vector
        // { "sort" : { "$hybrid" : { "$vector" : {"$binary": "c3VyZS4="} } } }
        var ejson = EJSONWrapper.maybeFrom(objectNode);
        if (ejson == null || ejson.type() != EJSONWrapper.EJSONType.BINARY) {
          // not the best error, wont tell people how to do a binary vector
          // and it will say it got an object node and that is not supported
          // TODO: better error message
          throw JsonFieldMatcher.errorForWrongType(
              jsonParser,
              ERROR_CONTEXT + " (binary vector)",
              LegMode.VECTOR.sortField(),
              objectNode,
              NullNode.class,
              ArrayNode.class,
              ObjectNode.class);
        }
        commandFeatures.addFeature(LegMode.VECTOR.commandFeature());
        yield ejson.getVectorValueForBinary();
      }
      case JsonNode wrongType ->
          throw JsonFieldMatcher.errorForWrongType(
              jsonParser,
              ERROR_CONTEXT,
              LegMode.VECTOR.sortField(),
              wrongType,
              NullNode.class,
              ArrayNode.class,
              ObjectNode.class);
    };
  }

  /**
   * Normalise the sort string from the user.
   *
   * @param value Nullable string value from the user.
   * @return null, if the user provided null, or a blank string, otherwise the trimmed value.
   */
  private static String normalizedText(String value) {
    return switch (value) {
      case null -> null;
      case String s when s.isBlank() -> null;
      default -> value.trim();
    };
  }
}
