package io.stargate.sgv2.jsonapi.api.model.command.table.definition;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToJsonKey;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnType;
import java.util.LinkedHashMap;

/** Ordered map of columns in a table definition. */
public class ColumnsDef extends LinkedHashMap<String, ColumnType> {

  public ColumnsDef() {
    super();
  }

  public ColumnsDef(int initialCapacity) {
    super(initialCapacity);
  }

  public ColumnType put(CqlIdentifier column, ColumnType columnType) {
    return super.put(cqlIdentifierToJsonKey(column), columnType);
  }
}
