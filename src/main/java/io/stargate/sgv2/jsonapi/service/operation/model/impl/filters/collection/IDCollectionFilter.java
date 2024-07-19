package io.stargate.sgv2.jsonapi.service.operation.model.impl.filters.collection;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.DOC_ID;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.base.Preconditions;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.cqldriver.serializer.CQLBindValues;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.builder.BuiltCondition;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.builder.BuiltConditionPredicate;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.builder.ConditionLHS;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.builder.JsonTerm;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/** Filters db documents based on a document id field value */
public class IDCollectionFilter extends CollectionFilter {
  public enum Operator {
    EQ,
    NE,
    IN
  }

  // HACK AARON - referenced from ExpressionBuilder, restrict access
  public final Operator operator;
  private final List<DocumentId> values;

  public IDCollectionFilter(Operator operator, DocumentId value) {
    this(operator, List.of(value));
  }

  public IDCollectionFilter(Operator operator, List<DocumentId> values) {
    super(DOC_ID);
    this.operator = operator;
    this.values = values;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    IDCollectionFilter idFilter = (IDCollectionFilter) o;
    return operator == idFilter.operator && Objects.equals(values, idFilter.values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(operator, values);
  }

  @Override
  public BuiltCondition get() {
    // For Id filter we always use getALL() method
    return null;
  }

  public List<BuiltCondition> getAll() {
    switch (operator) {
      case EQ:
        this.indexUsage.primaryKeyTag = true;
        return List.of(
            BuiltCondition.of(
                ConditionLHS.column("key"),
                BuiltConditionPredicate.EQ,
                new JsonTerm(CQLBindValues.getDocumentIdValue(values.get(0)))));
      case NE:
        final DocumentId documentId = (DocumentId) values.get(0);
        if (documentId.value() instanceof BigDecimal numberId) {
          this.indexUsage.numberIndexTag = true;
          return List.of(
              BuiltCondition.of(
                  ConditionLHS.mapAccess("query_dbl_values", DOC_ID),
                  BuiltConditionPredicate.NEQ,
                  new JsonTerm(DOC_ID, numberId)));
        } else if (documentId.value() instanceof String strId) {
          this.indexUsage.textIndexTag = true;
          return List.of(
              BuiltCondition.of(
                  ConditionLHS.mapAccess("query_text_values", DOC_ID),
                  BuiltConditionPredicate.NEQ,
                  new JsonTerm(DOC_ID, strId)));
        } else {
          throw new JsonApiException(
              ErrorCode.UNSUPPORTED_FILTER_DATA_TYPE,
              String.format("Unsupported $ne operand value : %s", documentId.value()));
        }
      case IN:
        if (values.isEmpty()) return List.of();
        return values.stream()
            .map(
                v -> {
                  this.indexUsage.primaryKeyTag = true;
                  return BuiltCondition.of(
                      ConditionLHS.column("key"),
                      BuiltConditionPredicate.EQ,
                      new JsonTerm(CQLBindValues.getDocumentIdValue(v)));
                })
            .collect(Collectors.toList());
      default:
        throw new JsonApiException(
            ErrorCode.UNSUPPORTED_FILTER_OPERATION,
            String.format("Unsupported id column operation %s", operator));
    }
  }

  public DocumentId getSingularDocumentId() {
    Preconditions.checkArgument(values.size() == 1, "Expected a single value");
    return values.get(0);
  }

  @Override
  protected Optional<JsonNode> jsonNodeForNewDocument(JsonNodeFactory nodeFactory) {
    if (Operator.EQ.equals(operator)) {
      return Optional.of(toJsonNode(nodeFactory, values.get(0)));
    }
    return Optional.empty();
  }

  //    @Override
  //    public JsonNode asJson(JsonNodeFactory nodeFactory) {
  //        return DBFilterBase.getJsonNode(nodeFactory, values.get(0));
  //    }
  //
  //    @Override
  //    public boolean canAddField() {
  //        if (operator.equals(Operator.EQ)) {
  //            return true;
  //        } else {
  //            return false;
  //        }
  //    }
}
