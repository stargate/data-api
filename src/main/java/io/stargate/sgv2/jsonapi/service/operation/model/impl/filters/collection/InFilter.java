package io.stargate.sgv2.jsonapi.service.operation.model.impl.filters.collection;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.DATA_CONTAINS;
import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.DOC_ID;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.cql.builder.BuiltCondition;
import io.stargate.sgv2.jsonapi.service.cql.builder.Predicate;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.JsonTerm;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocValueHasher;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import java.math.BigDecimal;
import java.util.*;

/** non_id($in, $nin), _id($nin) */
public class InFilter extends CollectionFilterBase {
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

  public InFilter(Operator operator, String path, List<Object> arrayValue) {
    super(path);
    this.arrayValue = arrayValue;
    this.operator = operator;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    InFilter inFilter = (InFilter) o;
    return operator == inFilter.operator && Objects.equals(arrayValue, inFilter.arrayValue);
  }

  @Override
  public int hashCode() {
    return Objects.hash(arrayValue, operator);
  }

  @Override
  public BuiltCondition get() {
    throw new UnsupportedOperationException("For IN filter we always use getALL() method");
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
            this.indexUsage.textIndexTag = true;
            inResult.add(
                BuiltCondition.of(
                    BuiltCondition.LHS.mapAccess("query_text_values", this.getPath()),
                    Predicate.EQ,
                    new JsonTerm(this.getPath(), getHash(new DocValueHasher(), value))));
          } else if (value instanceof List) {
            // array element is array
            this.indexUsage.textIndexTag = true;
            inResult.add(
                BuiltCondition.of(
                    BuiltCondition.LHS.mapAccess("query_text_values", this.getPath()),
                    Predicate.EQ,
                    new JsonTerm(this.getPath(), getHash(new DocValueHasher(), value))));
          } else {
            this.indexUsage.arrayContainsTag = true;
            inResult.add(
                BuiltCondition.of(
                    BuiltCondition.LHS.column(DATA_CONTAINS),
                    Predicate.CONTAINS,
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
              this.indexUsage.textIndexTag = true;
              ninResults.add(
                  BuiltCondition.of(
                      BuiltCondition.LHS.mapAccess("query_text_values", this.getPath()),
                      Predicate.NEQ,
                      new JsonTerm(this.getPath(), getHash(new DocValueHasher(), value))));
            } else if (value instanceof List) {
              // array element is array
              this.indexUsage.textIndexTag = true;
              ninResults.add(
                  BuiltCondition.of(
                      BuiltCondition.LHS.mapAccess("query_text_values", this.getPath()),
                      Predicate.NEQ,
                      new JsonTerm(this.getPath(), getHash(new DocValueHasher(), value))));
            } else {
              this.indexUsage.arrayContainsTag = true;
              ninResults.add(
                  BuiltCondition.of(
                      BuiltCondition.LHS.column(DATA_CONTAINS),
                      Predicate.NOT_CONTAINS,
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
                this.indexUsage.numberIndexTag = true;
                BuiltCondition condition =
                    BuiltCondition.of(
                        BuiltCondition.LHS.mapAccess("query_dbl_values", DOC_ID),
                        Predicate.NEQ,
                        new JsonTerm(DOC_ID, numberId));
                conditions.add(condition);
              } else if (docIdValue instanceof String strId) {
                this.indexUsage.textIndexTag = true;
                BuiltCondition condition =
                    BuiltCondition.of(
                        BuiltCondition.LHS.mapAccess("query_text_values", DOC_ID),
                        Predicate.NEQ,
                        new JsonTerm(DOC_ID, strId));
                conditions.add(condition);
              } else {
                throw new JsonApiException(
                    ErrorCode.UNSUPPORTED_FILTER_DATA_TYPE,
                    String.format("Unsupported _id $nin operand value: %s", docIdValue));
              }
            }
          }
          return conditions;
        }
      default:
        throw new JsonApiException(
            ErrorCode.UNSUPPORTED_FILTER_OPERATION,
            String.format("Unsupported %s column operation %s", getPath(), operator));
    }
  }
}
