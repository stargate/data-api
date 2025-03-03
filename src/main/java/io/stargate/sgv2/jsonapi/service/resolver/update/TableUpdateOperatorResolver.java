package io.stargate.sgv2.jsonapi.service.resolver.update;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.*;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.exception.UpdateException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.query.ColumnAssignment;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDef;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import java.util.List;

public interface TableUpdateOperatorResolver {

  List<ColumnAssignment> resolve(TableSchemaObject table, ObjectNode arguments);

  default ApiColumnDef checkUpdateColumnExists(TableSchemaObject table, String column) {
    var columnIdentifier = CqlIdentifierUtil.cqlIdentifierFromUserInput(column);
    var apiColumnDef = table.apiTableDef().allColumns().get(columnIdentifier);
    if (apiColumnDef == null) {
      throw UpdateException.Code.UNKNOWN_TABLE_COLUMNS.get(
          errVars(
              table,
              map -> {
                map.put("unknownColumns", errFmt(columnIdentifier));
                map.put(
                    "allColumns",
                    errFmtColumnMetadata(table.tableMetadata().getColumns().values()));
              }));
    }
    return apiColumnDef;
  }
}
