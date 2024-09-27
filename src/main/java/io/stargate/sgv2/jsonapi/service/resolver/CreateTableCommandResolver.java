package io.stargate.sgv2.jsonapi.service.resolver;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateTableCommand;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.PrimaryKey;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.tables.CreateTableOperation;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataType;
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
    boolean ifNotExists = command.options() != null ? command.options().ifNotExists() : false;
    Map<String, ApiDataType> columnTypes =
        command.definition().columns().entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getApiDataType()));
    List<String> partitionKeys = Arrays.stream(command.definition().primaryKey().keys()).toList();

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
    String comment = "";
    return new CreateTableOperation(
        ctx, tableName, columnTypes, partitionKeys, clusteringKeys, ifNotExists, comment);
  }

  @Override
  public Class<CreateTableCommand> getCommandClass() {
    return CreateTableCommand.class;
  }
}
