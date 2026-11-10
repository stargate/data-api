package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.*;
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
import io.stargate.sgv2.jsonapi.metrics.CommandFeature;
import io.stargate.sgv2.jsonapi.metrics.CommandFeatures;
import io.stargate.sgv2.jsonapi.util.JsonFieldMatcher;
import java.io.IOException;
import java.util.*;

/**
 * {@link StdDeserializer} for the {@link FindAndRerankSortDefinition}.
 *
 * <p>It keeps the sort as the user sent it. It only checks that the sort is an object or null.
 * There is no validation.
 */
public class FindAndRerankSortClauseDeserializer
    extends StdDeserializer<FindAndRerankSortDefinition> {

  private static final String ERROR_CONTEXT = "`sort` clause";

  // when the sort has $hybrid it must be the only field at top level
  // can be string or object
  // { "sort" : { "$hybrid" : "i like cheese" } }
  // { "sort" : { "$hybrid" : { "$vectorize" : "i like cheese", "$lexical" : "cows"} } }
  // { "sort" : { "$hybrid" : { "$vector" : [1, 2, 3], "$lexical" : "cows"} } }
  private static final JsonFieldMatcher<JsonNode> MATCH_HYBRID_FIELD =
      new JsonFieldMatcher<>(JsonNode.class, List.of(HYBRID_FIELD), List.of());

  // user is specifying the sorts individually
  // can be
  // { "sort" : { "$hybrid" : { "$vectorize" : "i like cheese", "$lexical" : "cows"} } }
  // { "sort" : { "$hybrid" : { "$vector" : [1, 2, 3], "$lexical" : "cows"} } }
  // they could also set something to null to skip that read, that is handled by the resolver
  // later.
  private static final JsonFieldMatcher<JsonNode> MATCH_SORT_FIELDS =
      new JsonFieldMatcher<>(
          JsonNode.class,
          List.of(),
          List.of(LEXICAL_CONTENT_FIELD, VECTOR_EMBEDDING_TEXT_FIELD, VECTOR_EMBEDDING_FIELD));

  // the metrics feature for the fields of a single leg sort that have one, the same features the
  // $hybrid object records for these fields
  private static final Map<String, CommandFeature> SINGLE_LEG_FEATURES =
      Map.of(
          VECTOR_EMBEDDING_TEXT_FIELD, CommandFeature.VECTORIZE,
          VECTOR_EMBEDDING_FIELD, CommandFeature.VECTOR,
          LEXICAL_CONTENT_FIELD, CommandFeature.LEXICAL);

  public FindAndRerankSortClauseDeserializer() {
    super(FindAndRerankSortDefinition.class);
  }

  /** {@inheritDoc} */
  @Override
  public FindAndRerankSortDefinition deserialize(
      JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {

    return switch (deserializationContext.readTree(jsonParser)) {
      case NullNode ignored -> // this is {"sort" : null}
          FindAndRerankSortDefinition.NO_ARG_SORT;
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
   * @return The {@link FindAndRerankSortDefinition} that reflects the request without validation,
   *     e.g. checking that a vectorize or vector sort is provided but not both.
   */
  private static FindAndRerankSortDefinition deserialize(JsonParser jsonParser, ObjectNode sort)
      throws JsonMappingException {

    // { "sort" : { } }
    if (sort.isEmpty()) {
      return FindAndRerankSortDefinition.NO_ARG_SORT;
    }

    // no $hybrid, so a single leg sort that is kept as sent, e.g.
    // { "sort" : { "$lexical" : "cows" } } or { "sort" : { "age" : -1 } }
    if (!sort.has(HYBRID_FIELD)) {
      return new FindAndRerankSortDefinition.SingleLegDefinition(
          sort, singleLegCommandFeatures(sort));
    }

    // TODO: $hybrid is parsed here, so MCP users get its errors as "unable to convert value" or
    // "Internal error", without the error code. Parse it in FindAndRerankSortParser and keep only
    // the sort as sent here, see FindAndRerankSortDefinition
    var hybridMatch = MATCH_HYBRID_FIELD.matchAndThrow(sort, jsonParser, ERROR_CONTEXT);

    return switch (hybridMatch.matched().get(HYBRID_FIELD)) {
      case TextNode textNode ->
          new FindAndRerankSort.HybridText(
              normalizedText(textNode.asText()), CommandFeatures.of(CommandFeature.HYBRID));
      case ObjectNode objectNode -> deserializeHybridObject(jsonParser, objectNode);
      case JsonNode node ->
          throw JsonFieldMatcher.errorForWrongType(
              jsonParser, ERROR_CONTEXT, HYBRID_FIELD, node, TextNode.class, ObjectNode.class);
    };
  }

  private static FindAndRerankSort.HybridObject deserializeHybridObject(
      JsonParser jsonParser, ObjectNode hybridObject) throws JsonMappingException {

    var sortMatch = MATCH_SORT_FIELDS.matchAndThrow(hybridObject, jsonParser, ERROR_CONTEXT);
    CommandFeatures commandFeatures = CommandFeatures.of(CommandFeature.HYBRID);

    var vectorizeText =
        switch (sortMatch.matched().get(VECTOR_EMBEDDING_TEXT_FIELD)) {
          case null -> {
            // undefined in the object, up to the resolver to check if both vectorize and vector are
            // not set
            // { "sort" : { "$hybrid" : { }}
            yield null;
          }
          case NullNode ignored -> {
            // explict setting to null is allowed
            // { "sort" : { "$hybrid" : { "$vectorize" : null,
            commandFeatures.addFeature(CommandFeature.VECTORIZE);
            yield null;
          }
          case TextNode textNode -> {
            // { "sort" : { "$hybrid" : { "$vectorize" : "I like cheese",
            commandFeatures.addFeature(CommandFeature.VECTORIZE);
            yield normalizedText(textNode.asText());
          }
          case JsonNode node ->
              throw JsonFieldMatcher.errorForWrongType(
                  jsonParser,
                  ERROR_CONTEXT,
                  VECTOR_EMBEDDING_TEXT_FIELD,
                  node,
                  NullNode.class,
                  TextNode.class);
        };

    var lexicalText =
        switch (sortMatch.matched().get(LEXICAL_CONTENT_FIELD)) {
          case null -> {
            // undefined in the object, up to the resolver to check if valid
            // { "sort" : { "$hybrid" : { }}
            yield null;
          }
          case NullNode ignored -> {
            // explict setting to null is allowed
            // { "sort" : { "$hybrid" : { "$lexical" : null,
            commandFeatures.addFeature(CommandFeature.LEXICAL);
            yield null;
          }
          case TextNode textNode -> {
            // { "sort" : { "$hybrid" : { "$lexical" : "cheese",
            commandFeatures.addFeature(CommandFeature.LEXICAL);
            yield normalizedText(textNode.asText());
          }
          case JsonNode node ->
              throw JsonFieldMatcher.errorForWrongType(
                  jsonParser,
                  ERROR_CONTEXT,
                  LEXICAL_CONTENT_FIELD,
                  node,
                  NullNode.class,
                  TextNode.class);
        };

    var vector =
        switch (sortMatch.matched().get(VECTOR_EMBEDDING_FIELD)) {
          case null -> {
            // undefined in the object, up to the resolver to check if valid
            // { "sort" : { "$hybrid" : { }}
            yield null;
          }
          case NullNode ignored -> {
            // explict setting to null is allowed
            // { "sort" : { "$hybrid" : { "$vector" : null,
            commandFeatures.addFeature(CommandFeature.VECTOR);
            yield null;
          }
          case ArrayNode arrayNode -> {
            // { "sort" : { "$hybrid" : { "vector" : [1,2,3],
            commandFeatures.addFeature(CommandFeature.VECTOR);
            yield arrayNodeToVector(arrayNode);
          }
          case ObjectNode objectNode -> {
            // binary vector
            // { "sort" : { "$hybrid" : { "$vector" : {"$binary": "c3VyZS4="},
            var ejson = EJSONWrapper.maybeFrom(objectNode);
            if (ejson == null || ejson.type() != EJSONWrapper.EJSONType.BINARY) {
              // not the best error, wont tell people how to do a binary vector
              // and it will say it got an object node and that is not supported
              // TODO: better error message
              throw JsonFieldMatcher.errorForWrongType(
                  jsonParser,
                  ERROR_CONTEXT + " (binary vector)",
                  VECTOR_EMBEDDING_FIELD,
                  objectNode,
                  NullNode.class,
                  ArrayNode.class,
                  ObjectNode.class);
            }
            commandFeatures.addFeature(CommandFeature.VECTOR);
            yield ejson.getVectorValueForBinary();
          }
          case JsonNode node ->
              throw JsonFieldMatcher.errorForWrongType(
                  jsonParser,
                  ERROR_CONTEXT,
                  VECTOR_EMBEDDING_FIELD,
                  node,
                  NullNode.class,
                  ArrayNode.class,
                  ObjectNode.class);
        };

    return new FindAndRerankSort.HybridObject(vectorizeText, lexicalText, vector, commandFeatures);
  }

  /** The metrics features of a single leg sort, from its field names only. */
  private static CommandFeatures singleLegCommandFeatures(ObjectNode sort) {
    var commandFeatures = CommandFeatures.create();
    SINGLE_LEG_FEATURES.forEach(
        (field, feature) -> {
          if (sort.has(field)) {
            commandFeatures.addFeature(feature);
          }
        });
    return commandFeatures;
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
