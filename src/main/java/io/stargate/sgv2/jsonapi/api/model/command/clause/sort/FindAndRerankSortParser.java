package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.stargate.sgv2.jsonapi.api.model.command.builders.CollectionSortClauseBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.builders.SortClauseBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression2.FieldSortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression2.LexicalSortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression2.VectorSortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression2.VectorizeSortExpression;
import io.stargate.sgv2.jsonapi.exception.SortException;
import io.stargate.sgv2.jsonapi.util.JsonUtil;
import io.stargate.sgv2.jsonapi.util.StringUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Parses the sort of a findAndRerank command into a {@link FindAndRerankSort}.
 *
 * <p>It checks only the syntax of the sort, so it does not need the collection or the config.
 *
 * <p>A sort with $hybrid must have a value for the vector leg: the text, $vectorize or $vector.
 *
 * <p>A sort without $hybrid uses the find sort syntax. For field sorts and $binary vectors it calls
 * the checks find uses, in {@link CollectionSortClauseBuilder} and {@link SortClauseBuilder}.
 *
 * <p>NOTE: find checks $lexical, $vector and $vectorize in the same method as its lexical index
 * check, so this class has its own copy of those checks. The copy differs from find in three ways:
 * $lexical must not be blank, a $lexical that is not text gets SORT_CLAUSE_VALUE_INVALID, and the
 * text of $lexical and $vectorize is trimmed. TODO: when find moves to {@link SortExpression2},
 * find should parse its sort here and keep only its collection checks. Then the copy is not needed.
 */
public final class FindAndRerankSortParser {

  /** The fields a $hybrid object can have. */
  private static final Set<String> HYBRID_OBJECT_FIELDS =
      Set.of(VECTOR_EMBEDDING_TEXT_FIELD, VECTOR_EMBEDDING_FIELD, LEXICAL_CONTENT_FIELD);

  private FindAndRerankSortParser() {
    // static methods only
  }

  /**
   * Parses the sort, throws a {@link SortException} when its syntax is not valid.
   *
   * @param definition The sort as the user sent it.
   * @return The parsed sort.
   */
  public static FindAndRerankSort parse(FindAndRerankSortDefinition definition) {
    Objects.requireNonNull(definition, "definition must not be null");
    var sort = definition.json();

    // no sort or a null sort: the filter leg, without sorting
    if (sort.isMissingNode() || sort.isNull()) {
      return new FindAndRerankSort.FilterLeg(List.of());
    }
    if (!(sort instanceof ObjectNode sortObject)) {
      throw SortException.Code.SORT_CLAUSE_INVALID.get(
          Map.of(
              "problem",
              "sort clause must be submitted as JSON Object, not %s"
                  .formatted(JsonUtil.nodeTypeAsString(sort))));
    }
    return sortObject.has(HYBRID_FIELD) ? parseHybrid(sortObject) : parseSingleLeg(sortObject);
  }

  // ===========================================================================================
  // $hybrid
  // ===========================================================================================

  /** <code>{"$hybrid": "cheese"}</code>, or a $hybrid object with a value for each leg. */
  private static FindAndRerankSort.MultiLeg parseHybrid(ObjectNode sort) {
    return switch (onlyField(sort, HYBRID_FIELD)) {
      case TextNode textNode -> hybridText(textNode.textValue());
      case ObjectNode objectNode -> hybridObject(objectNode);
      case JsonNode node ->
          throw SortException.Code.SORT_CLAUSE_VALUE_INVALID.get(
              Map.of(
                  "path",
                  HYBRID_FIELD,
                  "problem",
                  "value must be a String or an Object, not %s"
                      .formatted(JsonUtil.nodeTypeAsString(node))));
    };
  }

  /** The same text for both legs. It must not be blank. */
  private static FindAndRerankSort.HybridText hybridText(String text) {
    if (text.isBlank()) {
      throw SortException.Code.MISSING_HYBRID_SORT_VALUE.get();
    }
    return new FindAndRerankSort.HybridText(text.trim());
  }

  /**
   * <code>{"$hybrid": {"$vectorize": "cheese", "$lexical": "cows"}}</code>, or with $vector. It
   * must have $vectorize or $vector.
   */
  private static FindAndRerankSort.HybridObject hybridObject(ObjectNode hybrid) {
    hybrid
        .fieldNames()
        .forEachRemaining(
            field -> {
              if (!HYBRID_OBJECT_FIELDS.contains(field)) {
                throw SortException.Code.SORT_CLAUSE_INVALID.get(
                    Map.of(
                        "problem",
                        "a %s object can only have %s, %s and %s, not '%s'"
                            .formatted(
                                HYBRID_FIELD,
                                VECTOR_EMBEDDING_TEXT_FIELD,
                                VECTOR_EMBEDDING_FIELD,
                                LEXICAL_CONTENT_FIELD,
                                field)));
              }
            });

    var vectorize = optionalText(hybrid, VECTOR_EMBEDDING_TEXT_FIELD);
    var lexical = optionalText(hybrid, LEXICAL_CONTENT_FIELD);
    var vector = optionalVector(hybrid);

    // without $vectorize or $vector there is nothing to sort the vector leg by, see GH #2577
    if (vectorize == null && vector == null) {
      throw lexical == null
          ? SortException.Code.MISSING_HYBRID_SORT_VALUE.get()
          : SortException.Code.MISSING_VECTOR_OR_VECTORIZE_IN_HYBRID_SORT.get();
    }
    return new FindAndRerankSort.HybridObject(vectorize, lexical, vector);
  }

  /**
   * The trimmed text of $vectorize or $lexical in a $hybrid object. It is <code>null</code> when
   * the field is missing, null or blank.
   */
  private static String optionalText(ObjectNode hybrid, String field) {
    var value = hybrid.get(field);
    if (value == null || value.isNull()) {
      return null;
    }
    if (!value.isTextual()) {
      throw SortException.Code.SORT_CLAUSE_VALUE_INVALID.get(
          Map.of(
              "path",
              hybridPath(field),
              "problem",
              "value must be a String or null, not %s"
                  .formatted(JsonUtil.nodeTypeAsString(value))));
    }
    var text = value.textValue();
    return text.isBlank() ? null : text.trim();
  }

  /**
   * The vector of $vector in a $hybrid object. It is <code>null</code> when the field is missing or
   * null.
   */
  private static float[] optionalVector(ObjectNode hybrid) {
    var value = hybrid.get(VECTOR_EMBEDDING_FIELD);
    if (value == null || value.isNull()) {
      return null;
    }
    return vector(hybridPath(VECTOR_EMBEDDING_FIELD), value);
  }

  /** The path of a field in a $hybrid object, for error messages, e.g. $hybrid.$vectorize. */
  private static String hybridPath(String field) {
    return HYBRID_FIELD + "." + field;
  }

  // ===========================================================================================
  // Single leg sorts
  // ===========================================================================================

  private static FindAndRerankSort.SingleLeg parseSingleLeg(ObjectNode sort) {

    // an empty sort: the filter leg, without sorting
    if (sort.isEmpty()) {
      return new FindAndRerankSort.FilterLeg(List.of());
    }

    // $lexical, $vector and $vectorize are checked in the same order as find, so a sort with more
    // than one of them gets the same error

    // { "sort" : { "$lexical" : "cows" } }
    if (sort.has(LEXICAL_CONTENT_FIELD)) {
      return new FindAndRerankSort.LexicalLeg(
          new LexicalSortExpression(text(sort, LEXICAL_CONTENT_FIELD)));
    }

    // { "sort" : { "$vector" : [0.1, 0.2, 0.3] } }
    if (sort.has(VECTOR_EMBEDDING_FIELD)) {
      return new FindAndRerankSort.VectorLeg(
          new VectorSortExpression(
              vector(VECTOR_EMBEDDING_FIELD, onlyField(sort, VECTOR_EMBEDDING_FIELD))));
    }

    // { "sort" : { "$vectorize" : "cheese" } }
    if (sort.has(VECTOR_EMBEDDING_TEXT_FIELD)) {
      return new FindAndRerankSort.VectorLeg(
          new VectorizeSortExpression(text(sort, VECTOR_EMBEDDING_TEXT_FIELD)));
    }

    // { "sort" : { "age" : -1, "name" : 1 } }
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

  /** The value of the field, which must be the only field in the sort. */
  private static JsonNode onlyField(ObjectNode sort, String field) {
    if (sort.size() > 1) {
      throw SortException.Code.SORT_CLAUSE_INVALID.get(
          Map.of(
              "problem",
              "when sorting by field '%s' no other sort expressions allowed".formatted(field)));
    }
    return sort.get(field);
  }

  /** The trimmed text of $lexical or $vectorize, which must not be blank. */
  private static String text(ObjectNode sort, String field) {
    var value = onlyField(sort, field);
    // textValue() is null when the value is not text
    var text = value.textValue();
    if (StringUtil.isNullOrBlank(text)) {
      throw SortException.Code.SORT_CLAUSE_VALUE_INVALID.get(
          Map.of(
              "path",
              field,
              "problem",
              "value must be a non-blank String, not %s".formatted(value)));
    }
    return text.trim();
  }

  /** A vector: an array of numbers, or a $binary vector. */
  private static float[] vector(String path, JsonNode value) {
    return switch (value) {
      case ArrayNode arrayNode -> JsonUtil.arrayNodeToVector(arrayNode);
      case ObjectNode objectNode -> SortClauseBuilder.tryDecodeBinaryVector(path, objectNode);
      case JsonNode node ->
          throw SortException.Code.SORT_CLAUSE_VALUE_INVALID.get(
              Map.of(
                  "path",
                  path,
                  "problem",
                  "value must be an Array of numbers or a $binary vector, not %s"
                      .formatted(JsonUtil.nodeTypeAsString(node))));
    };
  }
}
