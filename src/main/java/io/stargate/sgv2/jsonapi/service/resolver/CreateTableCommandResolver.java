package io.stargate.sgv2.jsonapi.service.resolver;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateTableCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.VectorizeConfig;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.PrimaryKey;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ComplexTypes;
import io.stargate.sgv2.jsonapi.config.DebugModeConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableMetadataUtils;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorConfig;
import io.stargate.sgv2.jsonapi.service.operation.*;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.tables.CreateTableAttemptBuilder;
import io.stargate.sgv2.jsonapi.service.operation.tables.KeyspaceDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataType;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@ApplicationScoped
public class CreateTableCommandResolver implements CommandResolver<CreateTableCommand> {
  @Inject ObjectMapper objectMapper;
  @Inject VectorizeConfigValidator validateVectorize;

  @Override
  public Operation resolveKeyspaceCommand(
      CommandContext<KeyspaceSchemaObject> ctx, CreateTableCommand command) {
    String tableName = command.name();
    boolean ifNotExists = command.options() != null ? command.options().ifNotExists() : false;
    Map<String, ApiDataType> columnTypes =
        command.definition().columns().entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getApiDataType()));
    List<String> partitionKeys = Arrays.stream(command.definition().primaryKey().keys()).toList();

    Map<String, VectorConfig.ColumnVectorDefinition.VectorizeConfig> vectorizeConfigMap =
        command.definition().columns().entrySet().stream()
            .filter(
                e ->
                    e.getValue() instanceof ComplexTypes.VectorType vt
                        && vt.getVectorConfig() != null)
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e -> {
                      ComplexTypes.VectorType vectorType = ((ComplexTypes.VectorType) e.getValue());
                      final VectorizeConfig vectorizeConfig = vectorType.getVectorConfig();
                      validateVectorize.validateService(vectorizeConfig, vectorType.getDimension());
                      return new VectorConfig.ColumnVectorDefinition.VectorizeConfig(
                          vectorizeConfig.provider(),
                          vectorizeConfig.modelName(),
                          vectorizeConfig.authentication(),
                          vectorizeConfig.parameters());
                    }));

    if (partitionKeys.isEmpty()) {
      throw SchemaException.Code.MISSING_PRIMARY_KEYS.get();
    }
    partitionKeys.forEach(
        key -> {
          if (!columnTypes.containsKey(key)) {
            throw SchemaException.Code.COLUMN_DEFINITION_MISSING.get(Map.of("column_name", key));
          }
        });

    List<PrimaryKey.OrderingKey> clusteringKeys =
        command.definition().primaryKey().orderingKeys() == null
            ? List.of()
            : Arrays.stream(command.definition().primaryKey().orderingKeys()).toList();

    clusteringKeys.forEach(
        key -> {
          if (!columnTypes.containsKey(key.column())) {
            throw SchemaException.Code.COLUMN_DEFINITION_MISSING.get(
                Map.of("column_name", key.column()));
          }
          if (partitionKeys.contains(key.column())) {
            throw SchemaException.Code.PRIMARY_KEY_DEFINITION_INCORRECT.get();
          }
        });

    // set to empty will be used when vectorize is  supported
    Map<String, String> customProperties =
        TableMetadataUtils.createCustomProperties(vectorizeConfigMap, objectMapper);

    var attempt =
        new CreateTableAttemptBuilder(0, ctx.schemaObject())
            .retryDelayMillis(
                ctx.getConfig(OperationsConfig.class).databaseConfig().ddlRetryDelayMillis())
            .maxRetries(ctx.getConfig(OperationsConfig.class).databaseConfig().ddlRetries())
            .tableName(tableName)
            .columnTypes(columnTypes)
            .partitionKeys(partitionKeys)
            .clusteringKeys(clusteringKeys)
            .ifNotExists(ifNotExists)
            .customProperties(customProperties)
            .build();
    var attempts = new OperationAttemptContainer<>(List.of(attempt));

    var pageBuilder =
        SchemaAttemptPage.<KeyspaceSchemaObject>builder()
            .debugMode(ctx.getConfig(DebugModeConfig.class).enabled())
            .useErrorObjectV2(ctx.getConfig(OperationsConfig.class).extendError());

    return new GenericOperation<>(
        attempts, pageBuilder, new KeyspaceDriverExceptionHandler(command));
  }

  @Override
  public Class<CreateTableCommand> getCommandClass() {
    return CreateTableCommand.class;
  }
}
