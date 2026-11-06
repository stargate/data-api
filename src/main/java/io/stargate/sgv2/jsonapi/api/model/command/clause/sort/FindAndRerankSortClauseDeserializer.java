package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.HYBRID_FIELD;
import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.LEXICAL_CONTENT_FIELD;
import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD;
import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD;
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
import io.stargate.sgv2.jsonapi.metrics.CommandFeature;
import io.stargate.sgv2.jsonapi.metrics.CommandFeatures;
import io.stargate.sgv2.jsonapi.util.JsonFieldMatcher;
import java.io.IOException;
import java.util.*;

/**
 * {@link StdDeserializer} for the {@link FindAndRerankSort}.
 *
 * <p>Checks the shape and value types of $hybrid. Other sorts retain their JSON values for the
 * planner to validate with find's sort builder.
 */
public class FindAndRerankSortClauseDeserializer extends StdDeserializer<FindAndRerankSort> {

  private static final String ERROR_CONTEXT = "`sort` clause";

  // $hybrid is the only top-level field for a multi-leg sort
  // { "sort" : { "$hybrid" : "i like cheese" } }
  // { "sort" : { "$hybrid" : { "$vectorize" : "i like cheese", "$lexical" : "cows"} } }
  // { "sort" : { "$hybrid" : { "$vector" : [1, 2, 3], "$lexical" : "cows"} } }
  private static final JsonFieldMatcher<JsonNode> MATCH_TOP_LEVEL_SORT_FIELDS =
      new JsonFieldMatcher<>(
          JsonNode.class,
          List.of(),
          List.of(
              SortField.HYBRID.sortField(),
              SortField.VECTORIZE.sortField(),
              SortField.VECTOR.sortField(),
              SortField.LEXICAL.sortField()));

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
              SortField.LEXICAL.sortField(),
              SortField.VECTORIZE.sortField(),
              SortField.VECTOR.sortField()));

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
   * Deserialize the sort clause, retaining non-hybrid values until the collection is available.
   *
   * @param sort The sort clause as an object node, no check for nulls.
   */
  private static FindAndRerankSort deserialize(JsonParser jsonParser, ObjectNode sort)
      throws JsonMappingException {

    if (!sort.has(HYBRID_FIELD)) {
      var commandFeatures = CommandFeatures.create();
      for (var sortField : SortField.values()) {
        if (sort.has(sortField.sortField())) {
          commandFeatures.addFeature(sortField.commandFeature());
        }
      }
      return new FindAndRerankSort.UnresolvedSingleLeg(sort, commandFeatures);
    }

    var sortMatch = MATCH_TOP_LEVEL_SORT_FIELDS.matchAndThrow(sort, jsonParser, ERROR_CONTEXT);
    if (sortMatch.matched().size() != 1) {
      throw SortException.Code.SORT_CLAUSE_INVALID.get(
          Map.of(
              "problem",
              "sort clause must have exactly one of $hybrid, $vectorize, $vector, $lexical, but got: "
                  + String.join(", ", sortMatch.matched().keySet())));
    }

    return switch (sort.get(HYBRID_FIELD)) {
      case TextNode textNode ->
          new FindAndRerankSort.HybridText(
              normalizedText(textNode.asText()),
              CommandFeatures.of(SortField.HYBRID.commandFeature()));
      case ObjectNode objectNode -> deserializeHybridObject(jsonParser, objectNode);
      case JsonNode node ->
          throw JsonFieldMatcher.errorForWrongType(
              jsonParser, ERROR_CONTEXT, HYBRID_FIELD, node, TextNode.class, ObjectNode.class);
    };
  }

  private static FindAndRerankSort deserializeHybridObject(
      JsonParser jsonParser, ObjectNode hybridObject) throws JsonMappingException {

    var sortMatch =
        MATCH_HYBRID_OBJECT_FIELDS.matchAndThrow(hybridObject, jsonParser, ERROR_CONTEXT);
    // NOTE: commandFeatures are only for metrics, the FindAndRerankPlanner decides what to run
    // from the sort values
    CommandFeatures commandFeatures = CommandFeatures.of(SortField.HYBRID.commandFeature());

    var vectorizeText =
        textSortValue(
            jsonParser,
            SortField.VECTORIZE,
            sortMatch.matched().get(SortField.VECTORIZE.sortField()),
            commandFeatures);
    var lexicalText =
        textSortValue(
            jsonParser,
            SortField.LEXICAL,
            sortMatch.matched().get(SortField.LEXICAL.sortField()),
            commandFeatures);
    var vector =
        vectorSortValue(
            jsonParser, sortMatch.matched().get(SortField.VECTOR.sortField()), commandFeatures);

    return new FindAndRerankSort.HybridObject(vectorizeText, lexicalText, vector, commandFeatures);
  }

  /**
   * A text sort value from inside the {@code $hybrid} object. The field and its metrics feature
   * both come from {@code sortField}, so they cannot mismatch.
   *
   * @param sortField {@link SortField#VECTORIZE} or {@link SortField#LEXICAL}.
   * @param node The field value, or {@code null} when the field is missing.
   * @param commandFeatures The mode's feature is added when the field was present, even if null.
   * @return The trimmed text, or {@code null} when the field is missing, JSON null or blank. This
   *     skips the leg inside {@code $hybrid}.
   */
  private static String textSortValue(
      JsonParser jsonParser, SortField sortField, JsonNode node, CommandFeatures commandFeatures)
      throws JsonMappingException {
    return switch (node) {
      case null -> {
        // undefined in the object, up to the resolver to check if valid
        // { "sort" : { "$hybrid" : { }}
        yield null;
      }
      case NullNode ignored -> {
        // explicit setting to null is allowed
        // { "sort" : { "$hybrid" : { "$vectorize" : null } } }
        commandFeatures.addFeature(sortField.commandFeature());
        yield null;
      }
      case TextNode textNode -> {
        // { "sort" : { "$hybrid" : { "$vectorize" : "I like cheese" } } }
        commandFeatures.addFeature(sortField.commandFeature());
        yield normalizedText(textNode.asText());
      }
      case JsonNode wrongType ->
          throw JsonFieldMatcher.errorForWrongType(
              jsonParser,
              ERROR_CONTEXT,
              sortField.sortField(),
              wrongType,
              NullNode.class,
              TextNode.class);
    };
  }

  /**
   * A {@code $vector} sort value from inside the {@code $hybrid} object.
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
        // { "sort" : { "$hybrid" : { "$vector" : null } } }
        commandFeatures.addFeature(SortField.VECTOR.commandFeature());
        yield null;
      }
      case ArrayNode arrayNode -> {
        // { "sort" : { "$hybrid" : { "$vector" : [1,2,3] } } }
        commandFeatures.addFeature(SortField.VECTOR.commandFeature());
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
              SortField.VECTOR.sortField(),
              objectNode,
              NullNode.class,
              ArrayNode.class,
              ObjectNode.class);
        }
        commandFeatures.addFeature(SortField.VECTOR.commandFeature());
        yield ejson.getVectorValueForBinary();
      }
      case JsonNode wrongType ->
          throw JsonFieldMatcher.errorForWrongType(
              jsonParser,
              ERROR_CONTEXT,
              SortField.VECTOR.sortField(),
              wrongType,
              NullNode.class,
              ArrayNode.class,
              ObjectNode.class);
    };
  }

  /** JSON sort field names and their metrics features. */
  private enum SortField {
    HYBRID(HYBRID_FIELD, CommandFeature.HYBRID),
    VECTORIZE(VECTOR_EMBEDDING_TEXT_FIELD, CommandFeature.VECTORIZE),
    VECTOR(VECTOR_EMBEDDING_FIELD, CommandFeature.VECTOR),
    LEXICAL(LEXICAL_CONTENT_FIELD, CommandFeature.LEXICAL);

    private final String sortField;
    private final CommandFeature commandFeature;

    SortField(String sortField, CommandFeature commandFeature) {
      this.sortField = sortField;
      this.commandFeature = commandFeature;
    }

    String sortField() {
      return sortField;
    }

    CommandFeature commandFeature() {
      return commandFeature;
    }
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
