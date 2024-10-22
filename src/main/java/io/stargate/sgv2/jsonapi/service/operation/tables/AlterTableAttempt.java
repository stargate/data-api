package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.AlterTableAddColumnEnd;
import com.datastax.oss.driver.api.querybuilder.schema.AlterTableStart;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.SchemaAttempt;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataType;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import java.util.List;
import java.util.Map;

/**
 * Represents an attempt to alter a table schema. The attempt can be for adding columns, dropping
 * columns, or updating extensions.
 */
public class AlterTableAttempt extends SchemaAttempt<TableSchemaObject> {
  private final AlterTableType alterTableType;
  private final Map<String, ApiDataType> columnsToAdd;
  private final List<String> columnsToDrop;
  private final Map<String, String> customProperties;

  protected AlterTableAttempt(
      int position,
      TableSchemaObject schemaObject,
      AlterTableType alterTableType,
      Map<String, ApiDataType> columnsToAdd,
      List<String> columnsToDrop,
      Map<String, String> customProperties,
      SchemaRetryPolicy retryPolicy) {
    super(position, schemaObject, retryPolicy);
    this.alterTableType = alterTableType;
    this.columnsToAdd = columnsToAdd;
    this.columnsToDrop = columnsToDrop;
    this.customProperties = customProperties;
    setStatus(OperationStatus.READY);
  }

  @Override
  protected SimpleStatement buildStatement() {
    final CqlIdentifier keyspace =
        CqlIdentifierUtil.cqlIdentifierFromUserInput(schemaObject.name().keyspace());
    final CqlIdentifier table =
        CqlIdentifierUtil.cqlIdentifierFromUserInput(schemaObject.name().table());
    AlterTableStart alterTableStart = SchemaBuilder.alterTable(keyspace, table);
    return switch (alterTableType) {
      case ADD_COLUMNS -> buildAddColumnsStatement(alterTableStart);
      case DROP_COLUMNS -> buildDropColumnsStatement(alterTableStart);
      case UPDATE_EXTENSIONS -> buildUpdateExtensionStatement(alterTableStart);
    };
  }

  private SimpleStatement buildAddColumnsStatement(AlterTableStart alterTableStart) {
    assert columnsToAdd != null && !columnsToAdd.isEmpty();
    AlterTableAddColumnEnd addColumnEnd = null;
    for (Map.Entry<String, ApiDataType> column : columnsToAdd.entrySet()) {
      DataType dataType = getCqlDataType(column.getValue());
      if (addColumnEnd == null) {
        addColumnEnd =
            alterTableStart.addColumn(
                CqlIdentifierUtil.cqlIdentifierFromUserInput(column.getKey()), dataType);
      } else {
        addColumnEnd =
            addColumnEnd.addColumn(
                CqlIdentifierUtil.cqlIdentifierFromUserInput(column.getKey()), dataType);
      }
    }
    return addColumnEnd.build();
  }

  private SimpleStatement buildDropColumnsStatement(AlterTableStart alterTableStart) {
    assert columnsToDrop != null && !columnsToDrop.isEmpty();
    return alterTableStart.dropColumns(columnsToDrop.toArray(new String[0])).build();
  }

  private SimpleStatement buildUpdateExtensionStatement(AlterTableStart alterTableStart) {
    assert customProperties != null && !customProperties.isEmpty();
    Map<String, String> extensions = encodeAsHexValue(customProperties);
    return alterTableStart.withOption("extensions", extensions).build();
  }
}
