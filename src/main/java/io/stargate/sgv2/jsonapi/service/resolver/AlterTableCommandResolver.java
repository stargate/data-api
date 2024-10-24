package io.stargate.sgv2.jsonapi.service.resolver;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.VectorType;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.AlterTableCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.AlterTableOperation;
import io.stargate.sgv2.jsonapi.api.model.command.impl.AlterTableOperationImpl;
import io.stargate.sgv2.jsonapi.api.model.command.impl.VectorizeConfig;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ComplexColumnType;
import io.stargate.sgv2.jsonapi.config.DebugModeConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedUserType;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableMetadataUtils;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorConfig;
import io.stargate.sgv2.jsonapi.service.operation.GenericOperation;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.OperationAttemptContainer;
import io.stargate.sgv2.jsonapi.service.operation.SchemaAttempt;
import io.stargate.sgv2.jsonapi.service.operation.SchemaAttemptPage;
import io.stargate.sgv2.jsonapi.service.operation.tables.AlterTableAttempt;
import io.stargate.sgv2.jsonapi.service.operation.tables.AlterTableAttemptBuilder;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataType;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataTypeDefs;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@ApplicationScoped
public class AlterTableCommandResolver implements CommandResolver<AlterTableCommand> {
  @Inject ObjectMapper objectMapper;
  @Inject VectorizeConfigValidator validateVectorize;

  @Override
  public Operation resolveTableCommand(
      CommandContext<TableSchemaObject> ctx, AlterTableCommand command) {

    final AlterTableOperation operation = command.operation();
    final SchemaAttempt.SchemaRetryPolicy schemaRetryPolicy =
        new SchemaAttempt.SchemaRetryPolicy(
            2,
            Duration.ofMillis(
                ctx.getConfig(OperationsConfig.class).databaseConfig().ddlRetryDelayMillis()));
    List<AlterTableAttempt> attempts =
        switch (operation) {
          case AlterTableOperationImpl.AddColumns ac ->
              handleAddColumns(ac, ctx.schemaObject(), schemaRetryPolicy);
          case AlterTableOperationImpl.DropColumns dc ->
              handleDropColumns(dc, ctx.schemaObject(), schemaRetryPolicy);
          case AlterTableOperationImpl.AddVectorize av ->
              handleAddVectorize(av, ctx.schemaObject(), schemaRetryPolicy);
          case AlterTableOperationImpl.DropVectorize dc ->
              handleDropVectorize(dc, ctx.schemaObject(), schemaRetryPolicy);
          default -> throw new IllegalStateException("Unexpected value: " + operation);
        };

    OperationAttemptContainer<TableSchemaObject, SchemaAttempt<TableSchemaObject>> container =
        new OperationAttemptContainer<>(true);
    container.addAll(attempts);

    var pageBuilder =
        SchemaAttemptPage.<TableSchemaObject>builder()
            .debugMode(ctx.getConfig(DebugModeConfig.class).enabled())
            .useErrorObjectV2(ctx.getConfig(OperationsConfig.class).extendError());

    return new GenericOperation<>(container, pageBuilder, new TableDriverExceptionHandler());
  }

  private List<AlterTableAttempt> handleAddColumns(
      AlterTableOperationImpl.AddColumns ac,
      TableSchemaObject schemaObject,
      SchemaAttempt.SchemaRetryPolicy schemaRetryPolicy) {

    TableMetadata tableMetadata = schemaObject.tableMetadata();
    List<AlterTableAttempt> alterTableAttempts = new ArrayList<>();

    // check the column doesn't exists
    // TODO: move this to the attempt builder / factory
    // TODO: the error should say what columns are in the table, and include ALL of the columns that
    // duplicates
    ac.columns()
        .keySet()
        .forEach(
            column -> {
              if (tableMetadata
                  .getColumn(CqlIdentifierUtil.cqlIdentifierFromUserInput(column))
                  .isPresent()) {
                throw SchemaException.Code.COLUMN_ALREADY_EXISTS.get(Map.of("column", column));
              }
            });

    // New columns to be added
    // TODO: AARON: this is where the bad user column types like list of map will be caught and
    // thrown
    // better error

    Map<String, ApiDataType> addColumns =
        ac.columns().entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e -> {
                      try {
                        return ApiDataTypeDefs.from(e.getValue());
                      } catch (UnsupportedUserType ex) {
                        throw new RuntimeException(ex);
                      }
                    }));

    // Vectorize config for the new columns
    Map<String, VectorConfig.ColumnVectorDefinition.VectorizeDefinition> vectorizeConfigMap =
        ac.columns().entrySet().stream()
            .filter(
                e ->
                    e.getValue() instanceof ComplexColumnType.ColumnVectorType vt
                        && vt.getVectorConfig() != null)
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e -> {
                      ComplexColumnType.ColumnVectorType vectorType =
                          ((ComplexColumnType.ColumnVectorType) e.getValue());
                      final VectorizeConfig vectorizeConfig = vectorType.getVectorConfig();
                      validateVectorize.validateService(
                          vectorizeConfig, vectorType.getDimensions());
                      return new VectorConfig.ColumnVectorDefinition.VectorizeDefinition(
                          vectorizeConfig.provider(),
                          vectorizeConfig.modelName(),
                          vectorizeConfig.authentication(),
                          vectorizeConfig.parameters());
                    }));

    final AlterTableAttempt addColumnsAttempt =
        new AlterTableAttemptBuilder(schemaObject, schemaRetryPolicy)
            .addColumns(addColumns)
            .build();
    if (!vectorizeConfigMap.isEmpty()) {
      // Reading existing vectorize config from the table metadata
      Map<String, String> existingExtensions = TableMetadataUtils.getExtensions(tableMetadata);
      Map<String, VectorConfig.ColumnVectorDefinition.VectorizeDefinition>
          existingVectorizeConfigMap =
              TableMetadataUtils.getVectorizeMap(existingExtensions, objectMapper);

      // Merge the new config to the existing vectorize config
      existingVectorizeConfigMap.putAll(vectorizeConfigMap);

      // New custom property to be updated
      final Map<String, String> customProperties =
          TableMetadataUtils.createCustomProperties(existingVectorizeConfigMap, objectMapper);
      final AlterTableAttempt addVectorizeProperties =
          new AlterTableAttemptBuilder(schemaObject, schemaRetryPolicy)
              .customProperties(customProperties)
              .build();
      // First execute the extension update for add columns
      alterTableAttempts.add(addVectorizeProperties);
    }
    alterTableAttempts.add(addColumnsAttempt);

    // Create the AlterData object
    return alterTableAttempts;
  }

  private List<AlterTableAttempt> handleDropColumns(
      AlterTableOperationImpl.DropColumns dc,
      TableSchemaObject schemaObject,
      SchemaAttempt.SchemaRetryPolicy schemaRetryPolicy) {
    List<AlterTableAttempt> alterTableAttempts = new ArrayList<>();
    TableMetadata tableMetadata = schemaObject.tableMetadata();
    List<String> dropColumns = dc.columns();
    // Validate the columns to be dropped are present
    List<CqlIdentifier> primaryKeys =
        tableMetadata.getPrimaryKey().stream().map(ColumnMetadata::getName).toList();
    for (String columnName : dropColumns) {
      CqlIdentifier column = CqlIdentifierUtil.cqlIdentifierFromUserInput(columnName);
      if (primaryKeys.contains(column)) {
        throw SchemaException.Code.COLUMN_CANNOT_BE_DROPPED.get(
            Map.of("reason", "Primary key column `%s` cannot be dropped".formatted(columnName)));
      }

      if (tableMetadata.getColumn(column).isEmpty()) {
        throw SchemaException.Code.COLUMN_NOT_FOUND.get(Map.of("column", columnName));
      }

      final Optional<IndexMetadata> first =
          tableMetadata.getIndexes().values().stream()
              .filter(indexMetadata -> indexMetadata.getTarget().equals(columnName))
              .findFirst();
      if (first.isPresent()) {
        throw SchemaException.Code.COLUMN_CANNOT_BE_DROPPED.get(
            Map.of(
                "reason",
                "Index exists on the column `%s`, drop `%s` index to drop the column"
                    .formatted(
                        columnName,
                        CqlIdentifierUtil.cqlIdentifierToMessageString(first.get().getName()))));
      }
    }

    // Reading existing vectorize config from the table metadata
    Map<String, String> existingExtensions = TableMetadataUtils.getExtensions(tableMetadata);
    Map<String, VectorConfig.ColumnVectorDefinition.VectorizeDefinition>
        existingVectorizeConfigMap =
            TableMetadataUtils.getVectorizeMap(existingExtensions, objectMapper);

    // Merge the new config to the existing vectorize config
    boolean updateVectorize = false;
    for (String column : dropColumns) {
      final VectorConfig.ColumnVectorDefinition.VectorizeDefinition remove =
          existingVectorizeConfigMap.remove(column);
      if (remove != null) {
        updateVectorize = true;
      }
    }

    // First should drop the columns
    AlterTableAttempt dropColumnsAttempt =
        new AlterTableAttemptBuilder(schemaObject, schemaRetryPolicy)
            .dropColumns(dropColumns)
            .build();
    alterTableAttempts.add(dropColumnsAttempt);
    // New custom property to be updated
    Map<String, String> customProperties;
    if (updateVectorize) {
      customProperties =
          TableMetadataUtils.createCustomProperties(existingVectorizeConfigMap, objectMapper);
      AlterTableAttempt dropVectorizeProperties =
          new AlterTableAttemptBuilder(schemaObject, schemaRetryPolicy)
              .customProperties(customProperties)
              .build();
      // Then drop the vectorize properties
      alterTableAttempts.add(dropVectorizeProperties);
    }
    return alterTableAttempts;
  }

  private List<AlterTableAttempt> handleAddVectorize(
      AlterTableOperationImpl.AddVectorize av,
      TableSchemaObject schemaObject,
      SchemaAttempt.SchemaRetryPolicy schemaRetryPolicy) {
    List<AlterTableAttempt> alterTableAttempts = new ArrayList<>();
    TableMetadata tableMetadata = schemaObject.tableMetadata();
    Map<String, VectorConfig.ColumnVectorDefinition.VectorizeDefinition> vectorizeConfigMap =
        new HashMap<>();
    // New columns to be added
    for (Map.Entry<String, VectorizeConfig> entry : av.columns().entrySet()) {
      CqlIdentifier columnName = CqlIdentifierUtil.cqlIdentifierFromUserInput(entry.getKey());
      final Optional<ColumnMetadata> column = tableMetadata.getColumn(columnName);
      column.orElseThrow(
          () -> SchemaException.Code.COLUMN_NOT_FOUND.get(Map.of("column", entry.getKey())));
      if (column.get().getType() instanceof VectorType vt) {
        final VectorizeConfig vectorizeConfig = entry.getValue();
        validateVectorize.validateService(vectorizeConfig, vt.getDimensions());
        VectorConfig.ColumnVectorDefinition.VectorizeDefinition dbVectorConfig =
            new VectorConfig.ColumnVectorDefinition.VectorizeDefinition(
                vectorizeConfig.provider(),
                vectorizeConfig.modelName(),
                vectorizeConfig.authentication(),
                vectorizeConfig.parameters());
        vectorizeConfigMap.put(entry.getKey(), dbVectorConfig);
      } else {
        throw SchemaException.Code.NON_VECTOR_TYPE_COLUMN.get(Map.of("column", entry.getKey()));
      }
    }

    // Reading existing vectorize config from the table metadata
    Map<String, String> existingExtensions = TableMetadataUtils.getExtensions(tableMetadata);
    Map<String, VectorConfig.ColumnVectorDefinition.VectorizeDefinition>
        existingVectorizeConfigMap =
            TableMetadataUtils.getVectorizeMap(existingExtensions, objectMapper);
    existingVectorizeConfigMap.putAll(vectorizeConfigMap);
    Map<String, String> customProperties =
        TableMetadataUtils.createCustomProperties(existingVectorizeConfigMap, objectMapper);

    alterTableAttempts.add(
        new AlterTableAttemptBuilder(schemaObject, schemaRetryPolicy)
            .customProperties(customProperties)
            .build());
    return alterTableAttempts;
  }

  private List<AlterTableAttempt> handleDropVectorize(
      AlterTableOperationImpl.DropVectorize dc,
      TableSchemaObject schemaObject,
      SchemaAttempt.SchemaRetryPolicy schemaRetryPolicy) {
    TableMetadata tableMetadata = schemaObject.tableMetadata();
    List<AlterTableAttempt> alterTableAttempts = new ArrayList<>();
    // Reading existing vectorize config from the table metadata
    Map<String, String> existingExtensions = TableMetadataUtils.getExtensions(tableMetadata);
    Map<String, VectorConfig.ColumnVectorDefinition.VectorizeDefinition>
        existingVectorizeConfigMap =
            TableMetadataUtils.getVectorizeMap(existingExtensions, objectMapper);

    // Merge the new config to the existing vectorize config
    boolean updateVectorize = false;
    for (String column : dc.columns()) {
      CqlIdentifier columnIdentifier = CqlIdentifierUtil.cqlIdentifierFromUserInput(column);
      if (tableMetadata.getColumn(columnIdentifier).isEmpty()) {
        throw SchemaException.Code.COLUMN_NOT_FOUND.get(Map.of("column", column));
      }
      final VectorConfig.ColumnVectorDefinition.VectorizeDefinition remove =
          existingVectorizeConfigMap.remove(column);
      if (remove != null) {
        updateVectorize = true;
      }
    }

    // New custom property to be updated
    if (updateVectorize) {
      Map<String, String> customProperties =
          TableMetadataUtils.createCustomProperties(existingVectorizeConfigMap, objectMapper);
      final AlterTableAttempt dropVectorizeProperties =
          new AlterTableAttemptBuilder(schemaObject, schemaRetryPolicy)
              .customProperties(customProperties)
              .build();
      alterTableAttempts.add(dropVectorizeProperties);
    }

    return alterTableAttempts;
  }

  @Override
  public Class<AlterTableCommand> getCommandClass() {
    return AlterTableCommand.class;
  }
}
