package io.stargate.sgv2.jsonapi.service.operation.filters.table;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.IndexUsage;
import io.stargate.sgv2.jsonapi.service.operation.filters.DBFilterBase;

public abstract class TableFilter extends DBFilterBase {
  // TODO- the path is the column name here, maybe rename ?
  protected TableFilter(String path) {
    super(path, IndexUsage.NO_OP);
  }
}
