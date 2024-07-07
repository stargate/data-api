package io.stargate.sgv2.jsonapi.service.operation.model.filters.table;

import io.stargate.sgv2.jsonapi.service.operation.model.builder.BuiltCondition;
import io.stargate.sgv2.jsonapi.service.operation.model.builder.BuiltConditionPredicate;
import io.stargate.sgv2.jsonapi.service.operation.model.builder.LiteralTerm;

abstract class ColumnTableFilter<T> extends TableFilter {

  /** The operations that can be performed to filter a column */
  public enum Operator {
    EQ(BuiltConditionPredicate.EQ);

    final BuiltConditionPredicate predicate;

    Operator(BuiltConditionPredicate predicate) {
      this.predicate = predicate;
    }
  }

  protected final Operator operator;
  protected final T columnValue;
  protected final LiteralTerm<T> columnValueTerm;

  protected ColumnTableFilter(String path, Operator operator, T columnValue) {
    super(path);
    this.columnValue = columnValue;
    this.columnValueTerm = new LiteralTerm<>(columnValue);
    this.operator = operator;
  }

  @Override
  public BuiltCondition get() {

    return BuiltCondition.of(path, operator.predicate, columnValueTerm);
  }
}
