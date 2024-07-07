package io.stargate.sgv2.jsonapi.service.operation.model.impl.filters.table;

import io.stargate.sgv2.jsonapi.service.operation.model.impl.filters.DBFilterBase;

public abstract class TableFilter extends DBFilterBase {
  // TODO- the path is the column name here, maybe rename ?
  protected TableFilter(String path) {
    super(path);
  }
}
