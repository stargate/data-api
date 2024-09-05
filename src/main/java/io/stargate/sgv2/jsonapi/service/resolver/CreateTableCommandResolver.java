package io.stargate.sgv2.jsonapi.service.resolver;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateTableCommand;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.PrimaryKey;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnType;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.tables.CreateTableOperation;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@ApplicationScoped
public class CreateTableCommandResolver implements CommandResolver<CreateTableCommand> {
  @Override
  public Operation resolveKeyspaceCommand(
      CommandContext<KeyspaceSchemaObject> ctx, CreateTableCommand command) {
    String tableName = command.name();
    Map<String, ColumnType> columnTypes =
        command.definition().columns().entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().type()));
    List<String> partitionKeys = Arrays.stream(command.definition().primaryKey().keys()).toList();

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
        command.definition().primaryKey().orderingKeys() == null
            ? List.of()
            : Arrays.stream(command.definition().primaryKey().orderingKeys()).toList();

    clusteringKeys.forEach(
        key -> {
          if (!columnTypes.containsKey(key.column())) {
            throw ErrorCodeV1.TABLE_COLUMN_DEFINITION_MISSING.toApiException("%s", key.column());
          }
        });

    // set to empty will be used when vectorize is  supported
    String comment = "";
    return new CreateTableOperation(
        ctx, tableName, columnTypes, partitionKeys, clusteringKeys, comment);
  }

  @Override
  public Class<CreateTableCommand> getCommandClass() {
    return CreateTableCommand.class;
  }
}
