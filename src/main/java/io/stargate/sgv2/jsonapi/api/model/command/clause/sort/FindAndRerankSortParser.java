package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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

/**
 * Parses the sort of a findAndRerank command, as the command stores it, into a {@link
 * FindAndRerankSort}.
 *
 * <p>The parser checks only the syntax of the sort, it does not use the collection or the config.
 * For example it checks that $lexical has text, not that the collection has a lexical index: the
 * checks that need the collection are made when the command is planned. The command resolver calls
 * the parser before the planner, so a syntax error in the sort is reported before the errors the
 * planner finds, such as a missing rerankOn.
 *
 * <p>A single leg sort, one without $hybrid, has the syntax of the find sort: $lexical, $vector or
 * $vectorize must be the only field in the sort, any other field is a field sort with the value
 * <code>1</code> or <code>-1</code>. The errors have the same {@link SortException} codes as find.
 * The checks for field sorts and for $binary vectors are the ones find uses, see {@link
 * CollectionSortClauseBuilder} and {@link SortClauseBuilder}.
 *
 * <p>NOTE: find checks $lexical, $vector and $vectorize in {@link CollectionSortClauseBuilder}, in
 * the same method as its lexical index check, which needs the collection, so they are checked again
 * here. Two differences from find are on purpose: $lexical must not be blank, and a $lexical that
 * is not text gets SORT_CLAUSE_VALUE_INVALID, the same as $vectorize, not SORT_CLAUSE_INVALID.
 * TODO: when find moves to {@link SortExpression2}, find should parse its sort here and keep only
 * its collection checks, then the copy of these checks in {@link CollectionSortClauseBuilder} goes.
 */
public final class FindAndRerankSortParser {

  private FindAndRerankSortParser() {
    // static methods only
  }

  /**
   * Parses the sort, throws a {@link SortException} when its syntax is not valid.
   *
   * @param definition The sort as the command stores it.
   * @return The parsed sort.
   */
  public static FindAndRerankSort parse(FindAndRerankSortDefinition definition) {
    Objects.requireNonNull(definition, "definition must not be null");

    return switch (definition) {
        // TODO: FindAndRerankSortClauseDeserializer still parses $hybrid, parse it here instead,
        // see FindAndRerankSortDefinition
      case FindAndRerankSort.MultiLeg multiLeg -> multiLeg;
      case FindAndRerankSortDefinition.SingleLegDefinition singleLeg ->
          parseSingleLeg(singleLeg.sort());
    };
  }

  private static FindAndRerankSort.SingleLeg parseSingleLeg(JsonNode sort) {

    // no sort, a null sort or an empty sort: the filter leg, without sorting
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
      return new FindAndRerankSort.VectorLeg(new VectorSortExpression(vector(sort)));
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

  /** The value of $lexical, $vector or $vectorize, which must be the only field in the sort. */
  private static JsonNode onlyField(JsonNode sort, String field) {
    if (sort.size() > 1) {
      throw SortException.Code.SORT_CLAUSE_INVALID.get(
          Map.of(
              "problem",
              "when sorting by field '%s' no other sort expressions allowed".formatted(field)));
    }
    return sort.get(field);
  }

  /** The trimmed text of $lexical or $vectorize, which must not be blank. */
  private static String text(JsonNode sort, String field) {
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

  /** The vector of $vector: an array of numbers, or a $binary vector. */
  private static float[] vector(JsonNode sort) {
    return switch (onlyField(sort, VECTOR_EMBEDDING_FIELD)) {
      case ArrayNode arrayNode -> JsonUtil.arrayNodeToVector(arrayNode);
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
}
