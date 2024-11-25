package io.stargate.sgv2.jsonapi.service.operation.tables;

import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.alterTable;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.schema.AlterTableAddColumnEnd;
import com.datastax.oss.driver.api.querybuilder.schema.AlterTableStart;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableExtensions;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.SchemaAttempt;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDefContainer;
import java.util.List;
import java.util.Map;

/**
 * Represents an attempt to alter a table schema. The attempt can be for adding columns, dropping
 * columns, or updating extensions.
 */
public class AlterTableAttempt extends SchemaAttempt<TableSchemaObject> {

  private final AlterTableType alterTableType;
  private final ApiColumnDefContainer columnsToAdd;
  private final List<CqlIdentifier> columnsToDrop;
  private final Map<String, String> customProperties;

  protected AlterTableAttempt(
      int position,
      TableSchemaObject schemaObject,
      AlterTableType alterTableType,
      ApiColumnDefContainer columnsToAdd,
      List<CqlIdentifier> columnsToDrop,
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

    var alterTableStart =
        alterTable(
            schemaObject.tableMetadata().getKeyspace(), schemaObject.tableMetadata().getName());
    return switch (alterTableType) {
      case ADD_COLUMNS -> buildAddColumnsStatement(alterTableStart);
      case DROP_COLUMNS -> buildDropColumnsStatement(alterTableStart);
      case UPDATE_EXTENSIONS -> buildUpdateExtensionStatement(alterTableStart);
    };
  }

  private SimpleStatement buildAddColumnsStatement(AlterTableStart alterTableStart) {

    AlterTableAddColumnEnd addColumnEnd = null;

    for (var apiColumnDef : columnsToAdd.values()) {

      addColumnEnd =
          (addColumnEnd == null)
              ? alterTableStart.addColumn(apiColumnDef.name(), apiColumnDef.type().cqlType())
              : addColumnEnd.addColumn(apiColumnDef.name(), apiColumnDef.type().cqlType());
    }
    return addColumnEnd.build();
  }

  private SimpleStatement buildDropColumnsStatement(AlterTableStart alterTableStart) {

    return alterTableStart.dropColumns(columnsToDrop.toArray(new CqlIdentifier[0])).build();
  }

  private SimpleStatement buildUpdateExtensionStatement(AlterTableStart alterTableStart) {

    var extensions = TableExtensions.toExtensions(customProperties);
    return alterTableStart
        .withOption(TableExtensions.TABLE_OPTIONS_EXTENSION_KEY.asInternal(), extensions)
        .build();
  }

  /**
   * Represents an attempt to alter a table schema. The attempt can be of 3 types adding columns,
   * dropping columns, or updating extensions.
   */
  public enum AlterTableType {
    ADD_COLUMNS,
    DROP_COLUMNS,
    UPDATE_EXTENSIONS;
  }
}
