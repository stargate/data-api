package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtJoin;
import static io.stargate.sgv2.jsonapi.util.ApiPropertyUtils.getOrDefault;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateIndexCommand;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.config.constants.TableDescDefaults;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.SchemaDBTaskPage;
import io.stargate.sgv2.jsonapi.service.operation.SchemaDBTask;
import io.stargate.sgv2.jsonapi.service.operation.tables.CreateIndexDBTask;
import io.stargate.sgv2.jsonapi.service.operation.tables.CreateIndexExceptionHandler;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskGroup;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskOperation;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiIndexType;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiRegularIndex;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Duration;
import java.util.Map;

/** Resolver for the {@link CreateIndexCommand}. */
@ApplicationScoped
public class CreateIndexCommandResolver implements CommandResolver<CreateIndexCommand> {

  @Override
  public Class<CreateIndexCommand> getCommandClass() {
    return CreateIndexCommand.class;
  }

  @Override
  public Operation<TableSchemaObject> resolveTableCommand(
      CommandContext<TableSchemaObject> commandContext, CreateIndexCommand command) {

    var indexType =
        command.indexType() == null
            ? ApiIndexType.REGULAR
            : ApiIndexType.fromApiName(command.indexType());

    if (indexType == null) {
      throw SchemaException.Code.UNKNOWN_INDEX_TYPE.get(
          Map.of(
              "knownTypes", errFmtJoin(ApiIndexType.values(), ApiIndexType::apiName),
              "unknownType", command.indexType()));
    }

    if (indexType != ApiIndexType.REGULAR) {
      throw SchemaException.Code.UNSUPPORTED_INDEX_TYPE.get(
          Map.of(
              "supportedTypes", ApiIndexType.REGULAR.apiName(),
              "unsupportedType", command.indexType()));
    }

    // TODO: we need a centralised way of creating retry policy.
    var taskBuilder =
        CreateIndexDBTask.builder(commandContext.schemaObject())
            .withIfNotExists(
                getOrDefault(
                    command.options(),
                    CreateIndexCommand.CreateIndexCommandOptions::ifNotExists,
                    TableDescDefaults.CreateIndexOptionsDefaults.IF_NOT_EXISTS))
            .withSchemaRetryPolicy(
                new SchemaDBTask.SchemaRetryPolicy(
                    commandContext.getConfig(OperationsConfig.class).databaseConfig().ddlRetries(),
                    Duration.ofMillis(
                        commandContext
                            .getConfig(OperationsConfig.class)
                            .databaseConfig()
                            .ddlRetryDelayMillis())));

    // this will throw APIException if the index is not supported
    var apiIndex =
        ApiRegularIndex.FROM_DESC_FACTORY.create(
            commandContext.schemaObject(), command.name(), command.definition());

    taskBuilder.withExceptionHandlerFactory(
        DefaultDriverExceptionHandler.Factory.withIdentifier(
            CreateIndexExceptionHandler::new, apiIndex.indexName()));

    var taskGroup = new TaskGroup<>(taskBuilder.build(apiIndex));

    return new TaskOperation<>(taskGroup, SchemaDBTaskPage.accumulator(commandContext));
  }
}
