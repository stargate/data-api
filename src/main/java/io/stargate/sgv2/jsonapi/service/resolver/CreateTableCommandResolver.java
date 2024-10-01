package io.stargate.sgv2.jsonapi.service.resolver;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateTableCommand;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.ColumnDataType;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.PrimaryKey;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnType;
import io.stargate.sgv2.jsonapi.config.DebugModeConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.*;
import io.stargate.sgv2.jsonapi.service.operation.tables.CreateTableAttempt;
import io.stargate.sgv2.jsonapi.service.operation.tables.KeyspaceDriverExceptionHandler;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@ApplicationScoped
public class CreateTableCommandResolver implements CommandResolver<CreateTableCommand> {
  @Inject ObjectMapper objectMapper;

  @Override
  public Operation resolveKeyspaceCommand(
      CommandContext<KeyspaceSchemaObject> ctx, CreateTableCommand command) {
    final String tableName = command.name();
    final Map<String, ColumnType> columnTypes =
        command.definition().columns().entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> columnType(e.getValue())));
    final PrimaryKey primaryKey = primaryKey(command.definition().primaryKey());
    final List<String> partitionKeys = Arrays.stream(primaryKey.keys()).toList();

    if (partitionKeys.isEmpty()) {
      throw ErrorCodeV1.TABLE_MISSING_PARTITIONING_KEYS.toApiException();
    }
    partitionKeys.forEach(
        key -> {
          if (!columnTypes.containsKey(key)) {
            throw ErrorCodeV1.TABLE_COLUMN_DEFINITION_MISSING.toApiException("%s", key);
          }
        });

    List<PrimaryKey.OrderingKey> clusteringKeys =
        primaryKey.orderingKeys() == null
            ? List.of()
            : Arrays.stream(primaryKey.orderingKeys()).toList();

    clusteringKeys.forEach(
        key -> {
          if (!columnTypes.containsKey(key.column())) {
            throw ErrorCodeV1.TABLE_COLUMN_DEFINITION_MISSING.toApiException("%s", key.column());
          }
        });

    // set to empty will be used when vectorize is  supported
    String comment = "";

    var attempt =
        new CreateTableAttempt(
            0,
            ctx.schemaObject(),
            ctx.getConfig(OperationsConfig.class).databaseConfig().ddlRetryDelayMillis(),
            2, // AARON - could not find a config for this
            tableName,
            columnTypes,
            partitionKeys,
            clusteringKeys,
            comment);
    var attempts = new OperationAttemptContainer<>(List.of(attempt));

    var pageBuilder =
        SchemaAttemptPage.<KeyspaceSchemaObject>builder()
            .debugMode(ctx.getConfig(DebugModeConfig.class).enabled())
            .useErrorObjectV2(ctx.getConfig(OperationsConfig.class).extendError());

    return new GenericOperation<>(attempts, pageBuilder, new KeyspaceDriverExceptionHandler());
  }

  @Override
  public Class<CreateTableCommand> getCommandClass() {
    return CreateTableCommand.class;
  }

  private ColumnType columnType(Object typeDef) {
    return objectMapper.convertValue(typeDef, ColumnDataType.class).type();
  }

  private PrimaryKey primaryKey(Object pkDef) {
    return objectMapper.convertValue(pkDef, PrimaryKey.class);
  }
}
