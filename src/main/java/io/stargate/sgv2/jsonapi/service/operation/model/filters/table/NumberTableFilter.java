package io.stargate.sgv2.jsonapi.service.operation.model.filters.table;

public class NumberTableFilter<T extends Number> extends ColumnTableFilter<T> {

  public NumberTableFilter(String path, Operator operator, T value) {
    super(path, operator, value);
  }
}
