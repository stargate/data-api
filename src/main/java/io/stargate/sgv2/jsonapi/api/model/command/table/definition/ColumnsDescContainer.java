package io.stargate.sgv2.jsonapi.api.model.command.table.definition;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToJsonKey;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.sgv2.jsonapi.api.model.command.table.SchemaDescription;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnDesc;
import java.util.LinkedHashMap;

/** Ordered map of columns in a table definition. */
public class ColumnsDescContainer extends LinkedHashMap<String, ColumnDesc> implements SchemaDescription {

  public ColumnsDescContainer() {
    super();
  }

  public ColumnsDescContainer(int initialCapacity) {
    super(initialCapacity);
  }

  public ColumnDesc put(CqlIdentifier column, ColumnDesc columnDesc) {
    return super.put(cqlIdentifierToJsonKey(column), columnDesc);
  }
}
