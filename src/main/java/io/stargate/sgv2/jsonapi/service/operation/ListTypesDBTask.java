package io.stargate.sgv2.jsonapi.service.operation;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToJsonKey;

import com.datastax.oss.driver.api.core.type.UserDefinedType;
import io.stargate.sgv2.jsonapi.api.model.command.table.SchemaDescSource;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlType;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskBuilder;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskRetryPolicy;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDef;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiUdtType;
import io.stargate.sgv2.jsonapi.service.schema.tables.TypeBindingPoint;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Attempt to list types in a keyspace. */
public class ListTypesDBTask extends MetadataDBTask<KeyspaceSchemaObject> {

  public ListTypesDBTask(
      int position,
      KeyspaceSchemaObject schemaObject,
      DefaultDriverExceptionHandler.Factory<KeyspaceSchemaObject> exceptionHandlerFactory) {
    super(position, schemaObject, TaskRetryPolicy.NO_RETRY, exceptionHandlerFactory);
    setStatus(TaskStatus.READY);
  }

  public static TaskBuilder.BasicTaskBuilder<ListTypesDBTask, KeyspaceSchemaObject> builder(
      KeyspaceSchemaObject schemaObject) {
    return new TaskBuilder.BasicTaskBuilder<>(schemaObject, ListTypesDBTask::new);
  }

  /**
   * Get type names from the keyspace metadata.
   *
   * @return List of type names.
   */
  @Override
  protected List<String> getNames() {

    // aaron - see the MetadataDBTask, need better control on when this is set
    Objects.requireNonNull(
        keyspaceMetadata, "keyspaceMetadata must be set before calling getNames");

    return keyspaceMetadata
        // get all types
        .getUserDefinedTypes()
        .values()
        .stream()
        .map(typeMetadata -> cqlIdentifierToJsonKey(typeMetadata.getName()))
        .toList();
  }

  /**
   * Get types schema for all in the keyspace.
   *
   * @return List of UDT columnDesc as Object.
   */
  @Override
  protected Object getSchema() {

    // aaron - see the MetadataDBTask, need better control on when this is set
    Objects.requireNonNull(
        keyspaceMetadata, "keyspaceMetadata must be set before calling getNames");

    // get all types
    var types = keyspaceMetadata.getUserDefinedTypes().values();
    List<Object> res = new ArrayList<>();
    for (UserDefinedType type : types) {
      try {
        var apiUdtType =
            ApiUdtType.FROM_CQL_FACTORY.create(TypeBindingPoint.TABLE_COLUMN, type, null);
        // need full inline schema desc
        // so UDT schemaDescription needs to be called with SchemaDescSource.DML_USAGE
        res.add(apiUdtType.getSchemaDescription(SchemaDescSource.DML_USAGE));
      } catch (UnsupportedCqlType e) {
        res.add(
            ApiColumnDef.FROM_CQL_FACTORY
                .createUnsupported(type)
                .getSchemaDescription(SchemaDescSource.DDL_USAGE));
      }
    }
    return res;
  }
}
