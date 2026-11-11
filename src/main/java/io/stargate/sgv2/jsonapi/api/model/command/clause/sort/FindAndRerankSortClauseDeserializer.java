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
import io.stargate.sgv2.jsonapi.api.model.command.builders.CollectionSortClauseBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.builders.SortClauseBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.EJSONWrapper;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression2.FieldSortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression2.LexicalSortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression2.VectorSortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression2.VectorizeSortExpression;
import io.stargate.sgv2.jsonapi.exception.SortException;
import io.stargate.sgv2.jsonapi.metrics.CommandFeature;
import io.stargate.sgv2.jsonapi.metrics.CommandFeatures;
import io.stargate.sgv2.jsonapi.util.JsonFieldMatcher;
import io.stargate.sgv2.jsonapi.util.JsonUtil;
import io.stargate.sgv2.jsonapi.util.StringUtil;
import java.io.IOException;
import java.util.*;

/**
 * {@link StdDeserializer} for the {@link FindAndRerankSort}.
 *
 * <p>NOTE1: some single leg checks are a copy of the checks in {@link CollectionSortClauseBuilder}.
 *
 * <ul>
 *   <li>The copied checks are for $lexical, $vector and $vectorize.
 *   <li>That class checks them in one method together with the lexical index of the collection.
 *       This class does not have the collection, so it cannot call that method.
 *   <li>For field sorts and $binary vectors, this class calls it.
 * </ul>
 *
 * <p>TODO: remove the copy when find and findAndRerank parse the sort with the same code.
 *
 * <p>NOTE2: an error thrown here reaches HTTP users with its error code and message. MCP users get
 * a different error:
 *
 * <ul>
 *   <li>quarkus-mcp-server converts the tool arguments with Jackson before it calls the tool.
 *   <li>The MCP server catches only a Jackson mapping error. It reports it as "Invalid argument
 *       [sort] - unable to convert value".
 *   <li>It does not catch any other error, such as a {@link SortException}. It reports the error as
 *       "Internal error" and logs it as an ERROR.
 *   <li>Neither error has the error code or tells the agent how to fix the request.
 *   <li>See <a
 *       href="https://github.com/quarkiverse/quarkus-mcp-server/blob/2.0.3/core/runtime/src/main/java/io/quarkiverse/mcp/server/runtime/FeatureManagerBase.java#L297-L314">FeatureManagerBase.handleParam()</a>
 *       in quarkus-mcp-server 2.0.3.
 * </ul>
 *
 * <p>TODO: parse the sort when the command is resolved. Then MCP users also get the error code and
 * message.
 */
public class FindAndRerankSortClauseDeserializer extends StdDeserializer<FindAndRerankSort> {

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
   * @return The {@link FindAndRerankSort} for the request.
   */
  private static FindAndRerankSort deserialize(JsonParser jsonParser, ObjectNode sort)
      throws JsonMappingException {
    if (sort.has(HYBRID_FIELD)) {
      return deserializeMultiLeg(jsonParser, sort);
    }
    return deserializeSingleLeg(sort);
  }

  /** Deserialize a sort with $hybrid, which must be the only field in the sort. */
  private static FindAndRerankSort.MultiLeg deserializeMultiLeg(
      JsonParser jsonParser, ObjectNode sort) throws JsonMappingException {
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

  /**
   * Deserialize a sort without $hybrid: $lexical, $vector or $vectorize must be the only field in
   * the sort, any other field is a field sort with the value <code>1</code> or <code>-1</code>.
   */
  private static FindAndRerankSort.SingleLeg deserializeSingleLeg(ObjectNode sort) {

    // $lexical, $vector and $vectorize are checked in the same order as find, so a sort with more
    // than one of them gets the same error

    // { "sort" : { "$lexical" : "cows" } }
    if (sort.has(LEXICAL_CONTENT_FIELD)) {
      return new FindAndRerankSort.LexicalLeg(
          new LexicalSortExpression(singleLegText(sort, LEXICAL_CONTENT_FIELD)));
    }

    // { "sort" : { "$vector" : [0.1, 0.2, 0.3] } }
    if (sort.has(VECTOR_EMBEDDING_FIELD)) {
      return new FindAndRerankSort.VectorLeg(new VectorSortExpression(singleLegVector(sort)));
    }

    // { "sort" : { "$vectorize" : "cheese" } }
    if (sort.has(VECTOR_EMBEDDING_TEXT_FIELD)) {
      return new FindAndRerankSort.VectorLeg(
          new VectorizeSortExpression(singleLegText(sort, VECTOR_EMBEDDING_TEXT_FIELD)));
    }

    // { "sort" : { "age" : -1, "name" : 1 } }
    // an empty sort { "sort" : { } } gives the filter leg with no sorts
    // every path is checked before any value, the same as find
    sort.fieldNames().forEachRemaining(CollectionSortClauseBuilder::validateSortExpressionPath);
    var sorts = new ArrayList<FieldSortExpression>(sort.size());
    for (var field : sort.properties()) {
      sorts.add(
          new FieldSortExpression(
              field.getKey(),
              CollectionSortClauseBuilder.parseAscending(field.getKey(), field.getValue())));
    }
    return new FindAndRerankSort.FilterLeg(sorts);
  }

  /** The value of $lexical, $vector or $vectorize, which must be the only field in the sort. */
  private static JsonNode onlySortField(ObjectNode sort, String field) {
    if (sort.size() > 1) {
      throw SortException.Code.SORT_CLAUSE_INVALID.get(
          Map.of(
              "problem",
              "when sorting by field '%s' no other sort expressions allowed".formatted(field)));
    }
    return sort.get(field);
  }

  /** The trimmed text of $lexical or $vectorize, which must not be blank. */
  private static String singleLegText(ObjectNode sort, String field) {
    var value = onlySortField(sort, field);
    // textValue() is null when the value is not text
    var text = normalizedText(value.textValue());
    // NOTE: checks for blank as well as null. The trimmed text can be blank when the value is not,
    // because trim() also removes control characters
    if (StringUtil.isNullOrBlank(text)) {
      throw SortException.Code.SORT_CLAUSE_VALUE_INVALID.get(
          Map.of(
              "path",
              field,
              "problem",
              "value must be a non-blank String, not %s".formatted(value)));
    }
    return text;
  }

  /** The vector of $vector: an array of numbers, or a $binary vector. */
  private static float[] singleLegVector(ObjectNode sort) {
    return switch (onlySortField(sort, VECTOR_EMBEDDING_FIELD)) {
      case ArrayNode arrayNode -> arrayNodeToVector(arrayNode);
      case ObjectNode objectNode ->
          SortClauseBuilder.tryDecodeBinaryVector(VECTOR_EMBEDDING_FIELD, objectNode);
      case JsonNode node ->
          throw SortException.Code.SORT_CLAUSE_VALUE_INVALID.get(
              Map.of(
                  "path",
                  VECTOR_EMBEDDING_FIELD,
                  "problem",
                  "value must be an Array of numbers or a $binary vector, not %s"
                      .formatted(JsonUtil.nodeTypeAsString(node))));
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
