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
 * {@link StdDeserializer} for the {@link FindAndRerankSort}.
 *
 * <p>Note: There is no validation, this is handled when resolving the command to an operation.
 */
public class FindAndRerankSortClauseDeserializer extends StdDeserializer<FindAndRerankSort> {

  private static final String ERROR_CONTEXT = "`sort` clause";

  // the only field we accept at top level is $hybrid
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
   * @return The {@link FindAndRerankSort} that reflects the request without validation, e.g.
   *     checking that a vectorize or vector sort is provided but not both.
   */
  private static FindAndRerankSort deserialize(JsonParser jsonParser, ObjectNode sort)
      throws JsonMappingException {

    // { "sort" : { } }
    if (sort.isEmpty()) {
      return FindAndRerankSort.NO_ARG_SORT;
    }

    var hybridMatch = MATCH_HYBRID_FIELD.matchAndThrow(sort, jsonParser, ERROR_CONTEXT);

    return switch (hybridMatch.matched().get(HYBRID_FIELD)) {
      case TextNode textNode -> {
        // using the same text for vectorize and for lexical, no vector
        var normalizedText = normalizedText(textNode.asText().trim());
        yield new FindAndRerankSort(
            normalizedText, normalizedText, null, CommandFeatures.of(CommandFeature.HYBRID));
      }
      case ObjectNode objectNode -> deserializeHybridObject(jsonParser, objectNode);
      case JsonNode node ->
          throw JsonFieldMatcher.errorForWrongType(
              jsonParser, ERROR_CONTEXT, HYBRID_FIELD, node, TextNode.class, ObjectNode.class);
    };
  }

  private static FindAndRerankSort deserializeHybridObject(
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
            yield normalizedText(textNode.asText().trim());
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
            yield normalizedText(textNode.asText().trim());
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

    return new FindAndRerankSort(vectorizeText, lexicalText, vector, commandFeatures);
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
