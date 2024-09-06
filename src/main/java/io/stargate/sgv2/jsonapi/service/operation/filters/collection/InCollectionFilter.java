package io.stargate.sgv2.jsonapi.service.operation.filters.collection;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.DATA_CONTAINS;
import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.DOC_ID;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.operation.builder.BuiltCondition;
import io.stargate.sgv2.jsonapi.service.operation.builder.BuiltConditionPredicate;
import io.stargate.sgv2.jsonapi.service.operation.builder.ConditionLHS;
import io.stargate.sgv2.jsonapi.service.operation.builder.JsonTerm;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocValueHasher;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentId;
import java.math.BigDecimal;
import java.util.*;

/** non_id($in, $nin), _id($nin) */
public class InCollectionFilter extends CollectionFilter {
  private final List<Object> arrayValue;
  // HACK AARON - REFERENCED from ExpressionBuilder should be private
  public final Operator operator;

  /**
   * DO not update the new document from an upsert because we are matching on a list of possible
   * values so do not know which to set the field to
   *
   * @param nodeFactory
   * @return
   */
  @Override
  protected Optional<JsonNode> jsonNodeForNewDocument(JsonNodeFactory nodeFactory) {
    return Optional.empty();
  }

  //    @Override
  //    public JsonNode asJson(JsonNodeFactory nodeFactory) {
  //        return DBFilterBase.getJsonNode(nodeFactory, arrayValue);
  //    }
  //
  //    @Override
  //    public boolean canAddField() {
  //        return false;
  //    }

  public enum Operator {
    IN,
    NIN,
  }

  public InCollectionFilter(Operator operator, String path, List<Object> arrayValue) {
    super(path);
    this.arrayValue = arrayValue;
    this.operator = operator;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    InCollectionFilter inFilter = (InCollectionFilter) o;
    return operator == inFilter.operator && Objects.equals(arrayValue, inFilter.arrayValue);
  }

  @Override
  public int hashCode() {
    return Objects.hash(arrayValue, operator);
  }

  @Override
  public BuiltCondition get() {
    throw ErrorCodeV1.SERVER_INTERNAL_ERROR.toApiException(
        "For IN filter we always use getALL() method");
  }

  public List<BuiltCondition> getAll() {
    List<Object> values = arrayValue;
    switch (operator) {
      case IN:
        if (values.isEmpty()) return List.of();
        final ArrayList<BuiltCondition> inResult = new ArrayList<>();
        for (Object value : values) {
          if (value instanceof Map) {
            // array element is sub_doc
            this.collectionIndexUsage.textIndexTag = true;
            inResult.add(
                BuiltCondition.of(
                    ConditionLHS.mapAccess("query_text_values", this.getPath()),
                    BuiltConditionPredicate.EQ,
                    new JsonTerm(this.getPath(), getHash(new DocValueHasher(), value))));
          } else if (value instanceof List) {
            // array element is array
            this.collectionIndexUsage.textIndexTag = true;
            inResult.add(
                BuiltCondition.of(
                    ConditionLHS.mapAccess("query_text_values", this.getPath()),
                    BuiltConditionPredicate.EQ,
                    new JsonTerm(this.getPath(), getHash(new DocValueHasher(), value))));
          } else {
            this.collectionIndexUsage.arrayContainsTag = true;
            inResult.add(
                BuiltCondition.of(
                    ConditionLHS.column(DATA_CONTAINS),
                    BuiltConditionPredicate.CONTAINS,
                    new JsonTerm(getHashValue(new DocValueHasher(), getPath(), value))));
          }
        }
        return inResult;
      case NIN:
        if (values.isEmpty()) return List.of();
        if (!this.getPath().equals(DOC_ID)) {
          final ArrayList<BuiltCondition> ninResults = new ArrayList<>();
          for (Object value : values) {
            if (value instanceof Map) {
              // array element is sub_doc
              this.collectionIndexUsage.textIndexTag = true;
              ninResults.add(
                  BuiltCondition.of(
                      ConditionLHS.mapAccess("query_text_values", this.getPath()),
                      BuiltConditionPredicate.NEQ,
                      new JsonTerm(this.getPath(), getHash(new DocValueHasher(), value))));
            } else if (value instanceof List) {
              // array element is array
              this.collectionIndexUsage.textIndexTag = true;
              ninResults.add(
                  BuiltCondition.of(
                      ConditionLHS.mapAccess("query_text_values", this.getPath()),
                      BuiltConditionPredicate.NEQ,
                      new JsonTerm(this.getPath(), getHash(new DocValueHasher(), value))));
            } else {
              this.collectionIndexUsage.arrayContainsTag = true;
              ninResults.add(
                  BuiltCondition.of(
                      ConditionLHS.column(DATA_CONTAINS),
                      BuiltConditionPredicate.NOT_CONTAINS,
                      new JsonTerm(getHashValue(new DocValueHasher(), getPath(), value))));
            }
          }
          return ninResults;
        } else {
          // can not use stream here, since lambda parameter casting is not allowed
          List<BuiltCondition> conditions = new ArrayList<>();
          for (Object value : values) {
            if (value instanceof DocumentId) {
              Object docIdValue = ((DocumentId) value).value();
              if (docIdValue instanceof BigDecimal numberId) {
                this.collectionIndexUsage.numberIndexTag = true;
                BuiltCondition condition =
                    BuiltCondition.of(
                        ConditionLHS.mapAccess("query_dbl_values", DOC_ID),
                        BuiltConditionPredicate.NEQ,
                        new JsonTerm(DOC_ID, numberId));
                conditions.add(condition);
              } else if (docIdValue instanceof String strId) {
                this.collectionIndexUsage.textIndexTag = true;
                BuiltCondition condition =
                    BuiltCondition.of(
                        ConditionLHS.mapAccess("query_text_values", DOC_ID),
                        BuiltConditionPredicate.NEQ,
                        new JsonTerm(DOC_ID, strId));
                conditions.add(condition);
              } else {
                throw ErrorCodeV1.UNSUPPORTED_FILTER_DATA_TYPE.toApiException(
                    "Unsupported _id $nin operand value: %s", docIdValue);
              }
            }
          }
          return conditions;
        }
      default:
        throw ErrorCodeV1.UNSUPPORTED_FILTER_OPERATION.toApiException(
            "Unsupported %s column operation %s", getPath(), operator);
    }
  }
}
