package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.Row;
import java.util.HashMap;
import java.util.Map;

/**
 * Extended cached Row for comparator This row is used in the collection with comparator to
 * repeatedly deserializing data
 */
public class ValueCachedRow {
  private final Row row;
  private final Map<CqlIdentifier, Object> cachedValues = new HashMap<>();

  public ValueCachedRow(Row row) {
    this.row = row;
  }

  public Row getRow() {
    return row;
  }

  public Object getObject(CqlIdentifier columnName) {
    return cachedValues.computeIfAbsent(columnName, key -> row.getObject(key));
  }
}
