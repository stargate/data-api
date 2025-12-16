package io.stargate.sgv2.jsonapi.service.operation.tables;

import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.alterTable;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.schema.AlterTableAddColumnEnd;
import com.datastax.oss.driver.api.querybuilder.schema.AlterTableStart;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableExtensions;
import io.stargate.sgv2.jsonapi.service.operation.SchemaDBTask;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskRetryPolicy;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDefContainer;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject;
import java.util.List;
import java.util.Map;

/**
 * Represents an attempt to alter a table schema. The attempt can be for adding columns, dropping
 * columns, or updating extensions.
 */
public class AlterTableDBTask extends SchemaDBTask<TableSchemaObject> {

  private final AlterTableType alterTableType;
  private final ApiColumnDefContainer columnsToAdd;
  private final List<CqlIdentifier> columnsToDrop;
  private final Map<String, String> customProperties;

  protected AlterTableDBTask(
      int position,
      TableSchemaObject schemaObject,
      DefaultDriverExceptionHandler.Factory<TableSchemaObject> exceptionHandlerFactory,
      AlterTableType alterTableType,
      ApiColumnDefContainer columnsToAdd,
      List<CqlIdentifier> columnsToDrop,
      Map<String, String> customProperties,
      TaskRetryPolicy retryPolicy) {
    super(position, schemaObject, retryPolicy, exceptionHandlerFactory);

    this.alterTableType = alterTableType;
    this.columnsToAdd = columnsToAdd;
    this.columnsToDrop = columnsToDrop;
    this.customProperties = customProperties;

    // because this attempt is kind of overloaded we want to check that the right fields are set
    alterTableType.validate(this);
    setStatus(TaskStatus.READY);
  }

  public static AlterTableDBTaskBuilder builder(TableSchemaObject schemaObject) {
    return new AlterTableDBTaskBuilder(schemaObject);
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

    void validate(AlterTableDBTask attempt) {
      switch (this) {
        case ADD_COLUMNS:
          if (attempt.columnsToAdd == null || attempt.columnsToAdd.isEmpty()) {
            throw new IllegalStateException("columnsToAdd must be non null and non empty");
          }
          break;
        case DROP_COLUMNS:
          if (attempt.columnsToDrop == null || attempt.columnsToDrop.isEmpty()) {
            throw new IllegalStateException("columnsToDrop must be non null and non empty");
          }
          break;
        case UPDATE_EXTENSIONS:
          if (attempt.customProperties == null || attempt.customProperties.isEmpty()) {
            throw new IllegalStateException("customProperties must be non null and non empty");
          }
          break;
      }
    }
  }
}
