package io.stargate.sgv2.jsonapi.service.shredding.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.processor.SchemaValidatable;
import io.stargate.sgv2.jsonapi.service.shredding.DocRowIdentifer;
import io.stargate.sgv2.jsonapi.service.shredding.WritableDocRow;
import java.util.Map;

public record WriteableTableRow(RowId id, Map<CqlIdentifier, Object> allColumnValues)
    implements WritableDocRow, SchemaValidatable {

  @Override
  public DocRowIdentifer docRowID() {
    return id();
  }

  @Override
  public void validateTable(CommandContext<TableSchemaObject> commandContext) {
    // UPTO
  }
}
