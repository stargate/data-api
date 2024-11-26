package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.*;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.CQL_IDENTIFIER_COMPARATOR;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.AlterTableCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.AlterTableOperation;
import io.stargate.sgv2.jsonapi.api.model.command.impl.AlterTableOperationImpl;
import io.stargate.sgv2.jsonapi.api.model.command.impl.VectorizeConfig;
import io.stargate.sgv2.jsonapi.config.DebugModeConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableExtensions;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.operation.GenericOperation;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.OperationAttemptContainer;
import io.stargate.sgv2.jsonapi.service.operation.SchemaAttempt;
import io.stargate.sgv2.jsonapi.service.operation.SchemaAttemptPage;
import io.stargate.sgv2.jsonapi.service.operation.tables.AlterTableAttempt;
import io.stargate.sgv2.jsonapi.service.operation.tables.AlterTableAttemptBuilder;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.schema.tables.*;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class AlterTableCommandResolver implements CommandResolver<AlterTableCommand> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AlterTableCommandResolver.class);

  @Inject ObjectMapper objectMapper;
  @Inject VectorizeConfigValidator validateVectorize;

  @Override
  public Operation resolveTableCommand(
      CommandContext<TableSchemaObject> ctx, AlterTableCommand command) {

    final AlterTableOperation operation = command.operation();

    // TODO: centralized way of getting the retry policy
    var schemaRetryPolicy =
        new SchemaAttempt.SchemaRetryPolicy(
            ctx.getConfig(OperationsConfig.class).databaseConfig().ddlRetries(),
            Duration.ofMillis(
                ctx.getConfig(OperationsConfig.class).databaseConfig().ddlRetryDelayMillis()));

    var builder = new AlterTableAttemptBuilder(ctx.schemaObject(), schemaRetryPolicy);

    // use sequential processing for the attempts, because we sometimes need to do multiple
    // statements
    OperationAttemptContainer<TableSchemaObject, SchemaAttempt<TableSchemaObject>> attempts =
        new OperationAttemptContainer<>(true);

    attempts.addAll(
        switch (operation) {
          case AlterTableOperationImpl.AddColumns ac ->
              handleAddColumns(builder, ctx.schemaObject(), ac);
          case AlterTableOperationImpl.DropColumns dc ->
              handleDropColumns(builder, ctx.schemaObject(), dc);
          case AlterTableOperationImpl.AddVectorize av ->
              handleAddVectorize(builder, ctx.schemaObject(), av);
          case AlterTableOperationImpl.DropVectorize dc ->
              handleDropVectorize(builder, ctx.schemaObject(), dc);
          default ->
              throw new IllegalStateException(
                  "Unexpected AlterTable Operation class: " + operation.getClass().getSimpleName());
        });

    var pageBuilder =
        SchemaAttemptPage.<TableSchemaObject>builder()
            .debugMode(ctx.getConfig(DebugModeConfig.class).enabled())
            .useErrorObjectV2(ctx.getConfig(OperationsConfig.class).extendError());

    return new GenericOperation<>(attempts, pageBuilder, new TableDriverExceptionHandler());
  }

  private List<AlterTableAttempt> handleAddColumns(
      AlterTableAttemptBuilder builder,
      TableSchemaObject tableSchemaObject,
      AlterTableOperationImpl.AddColumns addColumnsOperation) {

    // 1. "add":{}
    // 2. "add":{"columns":null}
    // 3  "add":{"columns":{}}
    if (addColumnsOperation.columns() == null || addColumnsOperation.columns().isEmpty()) {
      throw SchemaException.Code.MISSING_ALTER_TABLE_OPERATIONS.get(
          errVars(
              tableSchemaObject,
              map -> {
                map.put("missingTableOperation", "add");
              }));
    }

    var apiTableDef = tableSchemaObject.apiTableDef();
    // we can have multiple attempts to run if we need to also update the custom properties on the
    // table
    List<AlterTableAttempt> attempts = new ArrayList<>();
    var addedColumns =
        ApiColumnDefContainer.FROM_COLUMN_DESC_FACTORY.create(
            addColumnsOperation.columns(), validateVectorize);

    // TODO: move this to the attempt builder / factory
    var duplicateColumns =
        addedColumns.values().stream()
            .filter(apiTableDef.allColumns()::contains)
            .sorted(ApiColumnDef.NAME_COMPARATOR)
            .collect(Collectors.toList());

    if (!duplicateColumns.isEmpty()) {
      throw SchemaException.Code.CANNOT_ADD_EXISTING_COLUMNS.get(
          errVars(
              tableSchemaObject,
              map -> {
                map.put("allColumns", errFmtApiColumnDef(apiTableDef.allColumns().values()));
                map.put("duplicateColumns", errFmtApiColumnDef(duplicateColumns));
              }));
    }

    // TODO: WHERE are unsupported columns checked ?

    var addedVectorizeDef = addedColumns.getVectorizeDefs();
    // if there is some vectorize config we need to write it, to write it we need to get the
    // existing
    // because the map is frozen in CQL and is always fully overridden.
    if (!addedVectorizeDef.isEmpty()) {

      var existingVectorizeDef = apiTableDef.allColumns().getVectorizeDefs();
      existingVectorizeDef.putAll(addedVectorizeDef);

      // New custom property to be updated
      var customProperties =
          TableExtensions.createCustomProperties(existingVectorizeDef, objectMapper);
      // First execute the extension update for add columns
      // so if we fail to add this we do not end up with a column that has missing vectorize
      // definition
      attempts.add(builder.buildUpdateExtensions(customProperties));
    }

    attempts.add(builder.buildAddColumns(addedColumns));
    return attempts;
  }

  private List<AlterTableAttempt> handleDropColumns(
      AlterTableAttemptBuilder builder,
      TableSchemaObject tableSchemaObject,
      AlterTableOperationImpl.DropColumns dropColumnsOperation) {

    // 1. "drop":{}
    // 2. "drop":{"columns":null}
    // 3  "drop":{"columns":[]}
    if (dropColumnsOperation.columns() == null || dropColumnsOperation.columns().isEmpty()) {
      throw SchemaException.Code.MISSING_ALTER_TABLE_OPERATIONS.get(
          errVars(
              tableSchemaObject,
              map -> {
                map.put("missingTableOperation", "drop");
              }));
    }

    var apiTableDef = tableSchemaObject.apiTableDef();
    // have to run multiple attempts if a vectorized column is dropped
    List<AlterTableAttempt> attempts = new ArrayList<>();

    var droppedColumns =
        dropColumnsOperation.columns().stream()
            .map(CqlIdentifierUtil::cqlIdentifierFromUserInput)
            .toList();

    // Validation
    // TODO: move this validation into the builder like the other attempts do

    var droppedPrimaryKeys =
        droppedColumns.stream()
            .filter(apiTableDef.primaryKeys()::containsKey)
            .sorted(CQL_IDENTIFIER_COMPARATOR)
            .toList();
    if (!droppedPrimaryKeys.isEmpty()) {
      throw SchemaException.Code.CANNOT_DROP_PRIMARY_KEY_COLUMNS.get(
          errVars(
              tableSchemaObject,
              map -> {
                map.put("allColumns", errFmtApiColumnDef(apiTableDef.allColumns().values()));
                map.put("primaryKeys", errFmtApiColumnDef(apiTableDef.primaryKeys().values()));
                map.put("droppedColumns", errFmtCqlIdentifier(droppedPrimaryKeys));
              }));
    }

    var unknownColumns =
        droppedColumns.stream()
            .filter(c -> !apiTableDef.allColumns().containsKey(c))
            .sorted(CQL_IDENTIFIER_COMPARATOR)
            .toList();
    if (!unknownColumns.isEmpty()) {
      throw SchemaException.Code.CANNOT_DROP_UNKNOWN_COLUMNS.get(
          errVars(
              tableSchemaObject,
              map -> {
                map.put("allColumns", errFmtApiColumnDef(apiTableDef.allColumns().values()));
                map.put("unknownColumns", errFmtCqlIdentifier(unknownColumns));
              }));
    }

    // TODO: Update when index def is on the table def
    var indexMetadataByTarget =
        tableSchemaObject.tableMetadata().getIndexes().values().stream()
            .collect(
                Collectors.toMap(
                    indexMetadata -> CqlIdentifier.fromInternal(indexMetadata.getTarget()),
                    Function.identity()));

    var droppedIndexes =
        droppedColumns.stream()
            .filter(indexMetadataByTarget::containsKey)
            .sorted(CQL_IDENTIFIER_COMPARATOR)
            .toList();
    if (!droppedIndexes.isEmpty()) {
      throw SchemaException.Code.CANNOT_DROP_INDEXED_COLUMNS.get(
          errVars(
              tableSchemaObject,
              map -> {
                map.put("allColumns", errFmtApiColumnDef(apiTableDef.allColumns().values()));
                map.put("indexedColumns", errFmtCqlIdentifier(indexMetadataByTarget.keySet()));
                map.put("droppedIndexedColumns", errFmtCqlIdentifier(droppedIndexes));
              }));
    }

    var existingVectorizeDefs = apiTableDef.allColumns().getVectorizeDefs();
    // Work out if we dropped anything that has a vectorize config
    boolean updateVectorize = false;
    for (var column : droppedColumns) {
      final VectorizeDefinition remove = existingVectorizeDefs.remove(column);
      if (remove != null) {
        updateVectorize = true;
      }
    }

    // First should drop the columns, ok to have a vectorize config without the column but not the
    // other way around
    attempts.add(builder.buildDropColumns(droppedColumns));

    // and then update the custom properties on the table if we changed the vectorize config
    if (updateVectorize) {
      attempts.add(
          builder.buildUpdateExtensions(
              TableExtensions.createCustomProperties(existingVectorizeDefs, objectMapper)));
    }
    return attempts;
  }

  private List<AlterTableAttempt> handleAddVectorize(
      AlterTableAttemptBuilder builder,
      TableSchemaObject tableSchemaObject,
      AlterTableOperationImpl.AddVectorize addVectorizeOperation) {

    // 1. "addVectorize":{}
    // 2. "addVectorize":{"columns":null}
    // 3  "addVectorize":{"columns":{}}
    if (addVectorizeOperation.columns() == null || addVectorizeOperation.columns().isEmpty()) {
      throw SchemaException.Code.MISSING_ALTER_TABLE_OPERATIONS.get(
          errVars(
              tableSchemaObject,
              map -> {
                map.put("missingTableOperation", "addVectorize");
              }));
    }

    var apiTableDef = tableSchemaObject.apiTableDef();

    // First need to get the definition of the column, because we need to get the dimensions of the
    // vector
    Map<CqlIdentifier, VectorizeConfig> addedVectorizeDesc =
        addVectorizeOperation.columns().entrySet().stream()
            .collect(
                Collectors.toMap(
                    entry -> cqlIdentifierFromUserInput(entry.getKey()), Map.Entry::getValue));

    var unknownColumns =
        addedVectorizeDesc.keySet().stream()
            .filter(c -> !apiTableDef.allColumns().containsKey(c))
            .sorted(CQL_IDENTIFIER_COMPARATOR)
            .toList();
    if (!unknownColumns.isEmpty()) {
      throw SchemaException.Code.CANNOT_VECTORIZE_UNKNOWN_COLUMNS.get(
          errVars(
              tableSchemaObject,
              map -> {
                map.put("allColumns", errFmtApiColumnDef(apiTableDef.allColumns().values()));
                map.put("unknownColumns", errFmtCqlIdentifier(unknownColumns));
              }));
    }

    var vectorColumns = apiTableDef.allColumns().filterBy(ApiTypeName.VECTOR);
    var nonVectorColumns =
        addedVectorizeDesc.keySet().stream()
            .filter(identifier -> !vectorColumns.containsKey(identifier))
            .sorted(CQL_IDENTIFIER_COMPARATOR)
            .toList();
    if (!nonVectorColumns.isEmpty()) {
      throw SchemaException.Code.CANNOT_VECTORIZE_NON_VECTOR_COLUMNS.get(
          errVars(
              tableSchemaObject,
              map -> {
                map.put("allColumns", errFmtApiColumnDef(apiTableDef.allColumns().values()));
                map.put("vectorColumns", errFmtApiColumnDef(vectorColumns.values()));
                map.put("nonVectorColumns", errFmtCqlIdentifier(nonVectorColumns));
              }));
    }

    // now should only be trying to vectorize columns that exist and are vectors
    Map<CqlIdentifier, VectorizeDefinition> addedVectorizeDefs =
        addedVectorizeDesc.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> {
                      var apiType = (ApiVectorType) vectorColumns.get(entry.getKey()).type();
                      return VectorizeDefinition.from(
                          entry.getValue(), apiType.getDimension(), validateVectorize);
                    }));

    // Merge the new vectorize defs into the existing vectorize defs
    // because all defs are always overrwritten
    var existingVectorizeDefs = apiTableDef.allColumns().getVectorizeDefs();
    existingVectorizeDefs.putAll(addedVectorizeDefs);

    return List.of(
        builder.buildUpdateExtensions(
            TableExtensions.createCustomProperties(existingVectorizeDefs, objectMapper)));
  }

  private List<AlterTableAttempt> handleDropVectorize(
      AlterTableAttemptBuilder builder,
      TableSchemaObject tableSchemaObject,
      AlterTableOperationImpl.DropVectorize dropVectorizeOperation) {

    // 1. "dropVectorize":{}
    // 2. "dropVectorize":{"columns":null}
    // 3  "dropVectorize":{"columns":[]}
    if (dropVectorizeOperation.columns() == null || dropVectorizeOperation.columns().isEmpty()) {
      throw SchemaException.Code.MISSING_ALTER_TABLE_OPERATIONS.get(
          errVars(
              tableSchemaObject,
              map -> {
                map.put("dropVectorize", "add");
              }));
    }

    var apiTableDef = tableSchemaObject.apiTableDef();
    var existingVectorizeDefs = apiTableDef.allColumns().getVectorizeDefs();

    var droppedColumns =
        dropVectorizeOperation.columns().stream()
            .map(CqlIdentifierUtil::cqlIdentifierFromUserInput)
            .toList();

    var vectorColumns = apiTableDef.allColumns().filterBy(ApiTypeName.VECTOR);
    var unknownColumns =
        droppedColumns.stream()
            .filter(c -> !apiTableDef.allColumns().containsKey(c))
            .sorted(CQL_IDENTIFIER_COMPARATOR)
            .toList();
    if (!unknownColumns.isEmpty()) {
      throw SchemaException.Code.CANNOT_DROP_VECTORIZE_FROM_UNKNOWN_COLUMNS.get(
          errVars(
              tableSchemaObject,
              map -> {
                map.put("allColumns", errFmtApiColumnDef(apiTableDef.allColumns().values()));
                map.put("vectorColumns", errFmtApiColumnDef(vectorColumns.values()));
                map.put("unknownColumns", errFmtCqlIdentifier(unknownColumns));
              }));
    }

    var nonVectorColumns =
        droppedColumns.stream()
            .filter(identifier -> !vectorColumns.containsKey(identifier))
            .sorted(CQL_IDENTIFIER_COMPARATOR)
            .toList();
    if (!nonVectorColumns.isEmpty()) {
      throw SchemaException.Code.CANNOT_DROP_VECTORIZE_FROM_NON_VECTOR_COLUMNS.get(
          errVars(
              tableSchemaObject,
              map -> {
                map.put("allColumns", errFmtApiColumnDef(apiTableDef.allColumns().values()));
                map.put("vectorColumns", errFmtApiColumnDef(vectorColumns.values()));
                map.put("nonVectorColumns", errFmtCqlIdentifier(nonVectorColumns));
              }));
    }

    // Should only be dropping config from existing vector columns

    boolean updateVectorize = false;
    for (var identifier : droppedColumns) {
      if (existingVectorizeDefs.remove(identifier) != null) {
        updateVectorize = true;
      }
    }

    if (!updateVectorize) {
      // Nothing to do, there was no vecorize def for the column :)
      return List.of();
    }

    return List.of(
        builder.buildUpdateExtensions(
            TableExtensions.createCustomProperties(existingVectorizeDefs, objectMapper)));
  }

  @Override
  public Class<AlterTableCommand> getCommandClass() {
    return AlterTableCommand.class;
  }
}
