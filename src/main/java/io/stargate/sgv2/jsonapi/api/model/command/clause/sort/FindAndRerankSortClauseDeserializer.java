package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.schema.collections.DocumentPath;
import io.stargate.sgv2.jsonapi.service.schema.naming.NamingRules;

import java.io.IOException;
import java.util.*;
import java.util.function.Predicate;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.*;

/** {@link StdDeserializer} for the {@link FindAndRerankSort}.
 * <p>
 * Note: There is no validation, this is handled when resolving the command to an operation.
 * <p>
 * See {@link #deserialise(ObjectNode)} for examples of the JSON we support.
 * */
public class FindAndRerankSortClauseDeserializer extends StdDeserializer<FindAndRerankSort> {

  /** No-arg constructor explicitly needed. */
  public FindAndRerankSortClauseDeserializer() {
    super(FindAndRerankSort.class);
  }

  /** {@inheritDoc} */
  @Override
  public FindAndRerankSort deserialize(JsonParser parser, DeserializationContext ctxt) throws IOException {

    return switch (ctxt.readTree(parser)){
      case NullNode ignored-> // this is {"sort" : null}
          FindAndRerankSort.NO_ARG_SORT;
      case ObjectNode objectNode -> deserialise(objectNode);
      default -> throw ErrorCodeV1.INVALID_SORT_CLAUSE.toApiException("sort clause must be a JSON object");
    };
  }

  /**
   * Deserialise the sort clause from an object node.
   * @param sort  The sort clause as an object node, no check for nulls.
   * @return The {@link FindAndRerankSort} that reflects the request without validation, e.g. checking that
   * a vectorize or vector sort is provided but not both.
   */
  private static FindAndRerankSort deserialise(ObjectNode sort){

    // { "sort" : { } }
    if (sort.isEmpty()){
      return FindAndRerankSort.NO_ARG_SORT;
    }

    // the only field we accept at top level is $hybrid
    // can be string or object
    // { "sort" : { "$hybrid" : "i like cheese" } }
    // { "sort" : { "$hybrid" : { "vectorize" : "i like cheese", "$lexical" : "cows"} } }
    // { "sort" : { "$hybrid" : { "$vector" : [1, 2, 3], "$lexical" : "cows"} } }
    var hybridMatch = filterFields(JsonNode.class, sort, List.of(HYBRID_FIELD));

    if (!hybridMatch.unexpectedAndWrongTypeNames().isEmpty()){
      throw ErrorCodeV1.INVALID_SORT_CLAUSE.toApiException("sort clause must contain exactly one field, $hybrid, unexpected fields: %s", String.join(",", hybridMatch.unexpectedAndWrongTypeNames()));
    }

    var hybridField = hybridMatch.matched.get(HYBRID_FIELD);

    return switch (hybridField){
      case TextNode textNode -> {
        // useing the same text for vectorize and for lexical, no vector
        yield new FindAndRerankSort(textNode.asText(), textNode.asText(), null);
      }
      case ObjectNode objectNode -> {
        yield deserializeHybridObject(objectNode);
      }
      default ->  throw ErrorCodeV1.INVALID_SORT_CLAUSE.toApiException("$hybrid field must be a string or an object");
    };
  }

  private static FindAndRerankSort deserializeHybridObject(ObjectNode hybridObject){

    // user is speciyfing the sorts individually
    // can be
    // { "sort" : { "$hybrid" : { "vectorize" : "i like cheese", "$lexical" : "cows"} } }
    // { "sort" : { "$hybrid" : { "$vector" : [1, 2, 3], "$lexical" : "cows"} } }
    // they could also set something to null to skip that read, that is handled by the resolver later.
    var sortTerms = filterFields(JsonNode.class, hybridObject, List.of(LEXICAL_CONTENT_FIELD, VECTOR_EMBEDDING_TEXT_FIELD, VECTOR_EMBEDDING_FIELD));

    if (!sortTerms.unexpectedAndWrongTypeNames().isEmpty()){
      throw ErrorCodeV1.INVALID_SORT_CLAUSE.toApiException("sort clause may only contain, %s, unexpected fields: %s", String.join(",", sortTerms.expectedFields()), String.join(",", sortTerms.unexpectedAndWrongTypeNames()));
    }

    var vectorizeText = switch (sortTerms.matched().get(VECTOR_EMBEDDING_TEXT_FIELD)){
      case null -> {
        // undefined in the object, up to the resolver to check if both vectorize and vector are set
        // { "sort" : { "$hybrid" : { }}
        yield null;
      }
      case NullNode ignored -> {
        // explict setting to null is allowed
        // { "sort" : { "$hybrid" : { "$vectorize" : null,
        yield "";
      }
      case TextNode textNode -> {
        // { "sort" : { "$hybrid" : { "$vectorize" : "I like cheese",
        yield textNode.asText();
      }
      default ->  throw ErrorCodeV1.INVALID_SORT_CLAUSE.toApiException("%s field must be a string or null".formatted(VECTOR_EMBEDDING_TEXT_FIELD));
    };

    var lexicalText = switch (sortTerms.matched().get(LEXICAL_CONTENT_FIELD)){
      case null -> {
        // undefined in the object, up to the resolver to check if both vectorize and vector are set
        // { "sort" : { "$hybrid" : { }}
        yield null;
      }
      case NullNode ignored -> {
        // explict setting to null is allowed
        // { "sort" : { "$hybrid" : { "$lexical" : null,
        yield "";
      }
      case TextNode textNode -> {
        // { "sort" : { "$hybrid" : { "$lexical" : "cheese",
        yield textNode.asText();
      }
      default ->  throw ErrorCodeV1.INVALID_SORT_CLAUSE.toApiException("%s field must be a string or null".formatted(LEXICAL_CONTENT_FIELD));
    };

    var vector = switch (sortTerms.matched().get(VECTOR_EMBEDDING_FIELD)){
      case null -> {
        // undefined in the object, up to the resolver to check if both vectorize and vector are set
        // { "sort" : { "$hybrid" : { }}
        yield null;
      }
      case NullNode ignored -> {
        // explict setting to null is allowed
        // { "sort" : { "$hybrid" : { "$vector" : null,
        yield null;
      }
      case ArrayNode arrayNode -> {
        // { "sort" : { "$hybrid" : { "vector" : [1,2,3],
        yield arrayNodeToVector(arrayNode);
      }
      default ->  throw ErrorCodeV1.INVALID_SORT_CLAUSE.toApiException("%s field must be an array or null".formatted(VECTOR_EMBEDDING_FIELD));
    };

    return new FindAndRerankSort(vectorizeText, lexicalText, vector);
  }

  record FilteredNodeFields<T extends JsonNode>(
      Map<String, T> matched,
      Map<String, JsonNode> wrongType,
      Map<String, JsonNode> unexpectedFields,
      List<String> expectedFields){

    List<String> unexpectedAndWrongTypeNames(){
      if (unexpectedFields.isEmpty() && wrongType.isEmpty()){
        return List.of();
      }
      List<String> list = new ArrayList<>(unexpectedFields.keySet());
      list.addAll(wrongType.keySet());
      return list;
    }
  }

  private static <T extends JsonNode> FilteredNodeFields<T> filterFields(Class<T> type, JsonNode node, List<String> expectedFields){

    Map<String, T> matched = null;
    Map<String, JsonNode> wrongType = null;
    Map<String, JsonNode> unexpectedFields = null;

    Predicate<String> contains = expectedFields.size() > 3 ? new HashSet<>(expectedFields)::contains : expectedFields::contains;

    for (var entry : node.properties()){

      if (! contains.test(entry.getKey())){
        if (unexpectedFields == null){
          unexpectedFields = new HashMap<>();
        }
        unexpectedFields.put(entry.getKey(), entry.getValue());
      }

      if (type.isInstance(entry.getValue())){
        if (matched == null){
          // only setting the size for matching, as we assume that is the most likely.
          matched = new HashMap<>(node.properties().size());
        }
        // type checked above
        matched.put(entry.getKey(), (T)entry.getValue());
      } else {
        if (wrongType == null){
          wrongType = new HashMap<>();
        }
        wrongType.put(entry.getKey(), entry.getValue());
      }
    }

    return new FilteredNodeFields<>(
        matched == null ? Map.of() : matched ,
        wrongType == null ? Map.of() : wrongType,
        unexpectedFields == null ? Map.of() : unexpectedFields,
        expectedFields);
  }
  /**
   * TODO: this almost duplicates code in WriteableShreddedDocument.shredVector() but that does not
   * check the array elements, we MUST stop duplicating code like this
   */
  private static float[] arrayNodeToVector(ArrayNode arrayNode) {

    float[] arrayVals = new float[arrayNode.size()];
    if (arrayNode.isEmpty()) {
      throw ErrorCodeV1.SHRED_BAD_VECTOR_SIZE.toApiException();
    }

    for (int i = 0; i < arrayNode.size(); i++) {
      JsonNode element = arrayNode.get(i);
      if (!element.isNumber()) {
        throw ErrorCodeV1.SHRED_BAD_VECTOR_VALUE.toApiException();
      }
      arrayVals[i] = element.floatValue();
    }
    return arrayVals;
  }
}
