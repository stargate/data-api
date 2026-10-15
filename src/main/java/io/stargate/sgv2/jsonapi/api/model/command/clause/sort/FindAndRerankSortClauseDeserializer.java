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
 * <p>Note: There is no validation, this is handled when resolving the command to an operation. The
 * sort records what the user wrote: the text of <code>{"$hybrid": "text"}</code> is kept as the
 * hybrid text, the planner decides which legs read it.
 */
public class FindAndRerankSortClauseDeserializer extends StdDeserializer<FindAndRerankSort> {

  private static final String ERROR_CONTEXT = "`sort` clause";

  // top level has exactly one of $hybrid, $vectorize, $vector or $lexical
  // { "sort" : { "$hybrid" : "i like cheese" } }
  // { "sort" : { "$hybrid" : { "$vectorize" : "i like cheese", "$lexical" : "cows"} } }
  // { "sort" : { "$vectorize" : "i like cheese" } }
  // { "sort" : { "$vector" : [1, 2, 3] } }
  // { "sort" : { "$lexical" : "cows" } }
  private static final JsonFieldMatcher<JsonNode> MATCH_SORT_CLAUSE_FIELDS =
      new JsonFieldMatcher<>(
          JsonNode.class,
          List.of(),
          List.of(
              HYBRID_FIELD,
              VECTOR_EMBEDDING_TEXT_FIELD,
              VECTOR_EMBEDDING_FIELD,
              LEXICAL_CONTENT_FIELD));

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
   * @return The {@link FindAndRerankSort} that reflects the request without validation. For example
   *     a sort with both <code>$vectorize</code> and <code>$vector</code> keeps both values, and
   *     the planner then validates.
   */
  private static FindAndRerankSort deserialize(JsonParser jsonParser, ObjectNode sort)
      throws JsonMappingException {

    // { "sort" : { } }
    if (sort.isEmpty()) {
      return FindAndRerankSort.NO_ARG_SORT;
    }

    var sortMatch = MATCH_SORT_CLAUSE_FIELDS.matchAndThrow(sort, jsonParser, ERROR_CONTEXT);
    if (sortMatch.matched().size() != 1) {
      throw new JsonMappingException(
          jsonParser,
          "sort clause must have exactly one of $hybrid, $vectorize, $vector, $lexical",
          jsonParser.currentLocation());
    }

    if (sortMatch.matched().containsKey(HYBRID_FIELD)) {
      return switch (sortMatch.matched().get(HYBRID_FIELD)) {
        case TextNode textNode ->
            FindAndRerankSort.fromHybridText(
                normalizedText(textNode.asText().trim()),
                CommandFeatures.of(CommandFeature.HYBRID));
        case ObjectNode objectNode -> deserializeHybridObject(jsonParser, objectNode);
        case JsonNode node ->
            throw JsonFieldMatcher.errorForWrongType(
                jsonParser, ERROR_CONTEXT, HYBRID_FIELD, node, TextNode.class, ObjectNode.class);
      };
    }
    if (sortMatch.matched().containsKey(VECTOR_EMBEDDING_TEXT_FIELD)) {
      var commandFeatures = CommandFeatures.create();
      var vectorizeText =
          textSortValue(
              jsonParser,
              VECTOR_EMBEDDING_TEXT_FIELD,
              sortMatch.matched().get(VECTOR_EMBEDDING_TEXT_FIELD),
              commandFeatures,
              CommandFeature.VECTORIZE);
      return FindAndRerankSort.fromVectorizeSort(vectorizeText, commandFeatures);
    }
    if (sortMatch.matched().containsKey(LEXICAL_CONTENT_FIELD)) {
      var commandFeatures = CommandFeatures.create();
      var lexicalText =
          textSortValue(
              jsonParser,
              LEXICAL_CONTENT_FIELD,
              sortMatch.matched().get(LEXICAL_CONTENT_FIELD),
              commandFeatures,
              CommandFeature.LEXICAL);
      return FindAndRerankSort.fromLexicalSort(lexicalText, commandFeatures);
    }
    // only $vector is left, the matcher has checked the field names
    var commandFeatures = CommandFeatures.create();
    var vector =
        vectorSortValue(
            jsonParser, sortMatch.matched().get(VECTOR_EMBEDDING_FIELD), commandFeatures);
    return FindAndRerankSort.fromVectorSort(vector, commandFeatures);
  }

  private static FindAndRerankSort deserializeHybridObject(
      JsonParser jsonParser, ObjectNode hybridObject) throws JsonMappingException {

    var sortMatch = MATCH_SORT_FIELDS.matchAndThrow(hybridObject, jsonParser, ERROR_CONTEXT);
    // NOTE: commandFeatures are only for metrics, the FindAndRerankPlanner decides what to run
    // from the legMode and the sort values
    CommandFeatures commandFeatures = CommandFeatures.of(CommandFeature.HYBRID);

    var vectorizeText =
        textSortValue(
            jsonParser,
            VECTOR_EMBEDDING_TEXT_FIELD,
            sortMatch.matched().get(VECTOR_EMBEDDING_TEXT_FIELD),
            commandFeatures,
            CommandFeature.VECTORIZE);
    var lexicalText =
        textSortValue(
            jsonParser,
            LEXICAL_CONTENT_FIELD,
            sortMatch.matched().get(LEXICAL_CONTENT_FIELD),
            commandFeatures,
            CommandFeature.LEXICAL);
    var vector =
        vectorSortValue(
            jsonParser, sortMatch.matched().get(VECTOR_EMBEDDING_FIELD), commandFeatures);

    return FindAndRerankSort.fromHybridObject(vectorizeText, lexicalText, vector, commandFeatures);
  }

  // a text sort value, null when the field is missing, null or blank
  private static String textSortValue(
      JsonParser jsonParser,
      String field,
      JsonNode node,
      CommandFeatures commandFeatures,
      CommandFeature feature)
      throws JsonMappingException {
    return switch (node) {
      case null -> null;
      case NullNode ignored -> {
        commandFeatures.addFeature(feature);
        yield null;
      }
      case TextNode textNode -> {
        commandFeatures.addFeature(feature);
        yield normalizedText(textNode.asText().trim());
      }
      case JsonNode wrongType ->
          throw JsonFieldMatcher.errorForWrongType(
              jsonParser, ERROR_CONTEXT, field, wrongType, NullNode.class, TextNode.class);
    };
  }

  // a $vector sort value, null when the field is missing or null
  private static float[] vectorSortValue(
      JsonParser jsonParser, JsonNode node, CommandFeatures commandFeatures)
      throws JsonMappingException {
    return switch (node) {
      case null -> null;
      case NullNode ignored -> {
        commandFeatures.addFeature(CommandFeature.VECTOR);
        yield null;
      }
      case ArrayNode arrayNode -> {
        commandFeatures.addFeature(CommandFeature.VECTOR);
        yield arrayNodeToVector(arrayNode);
      }
      case ObjectNode objectNode -> {
        // binary vector, {"$binary": "c3VyZS4="}
        var ejson = EJSONWrapper.maybeFrom(objectNode);
        if (ejson == null || ejson.type() != EJSONWrapper.EJSONType.BINARY) {
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
      case JsonNode wrongType ->
          throw JsonFieldMatcher.errorForWrongType(
              jsonParser,
              ERROR_CONTEXT,
              VECTOR_EMBEDDING_FIELD,
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
