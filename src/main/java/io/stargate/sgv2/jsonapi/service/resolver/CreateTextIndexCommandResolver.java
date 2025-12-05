package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtJoin;
import static io.stargate.sgv2.jsonapi.util.ApiOptionUtils.getOrDefault;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateTextIndexCommand;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.config.constants.TableDescDefaults;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.SchemaDBTask;
import io.stargate.sgv2.jsonapi.service.operation.SchemaDBTaskPage;
import io.stargate.sgv2.jsonapi.service.operation.tables.CreateIndexDBTask;
import io.stargate.sgv2.jsonapi.service.operation.tables.CreateIndexDBTaskBuilder;
import io.stargate.sgv2.jsonapi.service.operation.tables.CreateIndexExceptionHandler;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskGroup;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskOperation;
import io.stargate.sgv2.jsonapi.service.schema.naming.NamingRules;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiIndexType;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTextIndex;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Duration;
import java.util.Map;

/** Resolver for the {@link CreateTextIndexCommand}. */
@ApplicationScoped
public class CreateTextIndexCommandResolver implements CommandResolver<CreateTextIndexCommand> {

  @Override
  public Class<CreateTextIndexCommand> getCommandClass() {
    return CreateTextIndexCommand.class;
  }

  @Override
  public Operation<TableSchemaObject> resolveTableCommand(
      CommandContext<TableSchemaObject> commandContext, CreateTextIndexCommand command) {

    var indexName = NamingRules.INDEX.checkRule(command.name());

    var indexType =
        command.indexType() == null
            ? ApiIndexType.TEXT
            : ApiIndexType.fromApiName(command.indexType());

    if (indexType == null) {
      throw SchemaException.Code.UNKNOWN_INDEX_TYPE.get(
          Map.of(
              "knownTypes",
              errFmtJoin(ApiIndexType.values(), ApiIndexType::apiName),
              "unknownType",
              command.indexType()));
    }

    if (indexType != ApiIndexType.TEXT) {
      throw SchemaException.Code.UNSUPPORTED_INDEX_TYPE.get(
          Map.of(
              "supportedTypes",
              ApiIndexType.TEXT.apiName(),
              "unsupportedType",
              command.indexType()));
    }

    // TODO: we need a centralised way of creating retry attempt.
    CreateIndexDBTaskBuilder taskBuilder =
        CreateIndexDBTask.builder(commandContext.schemaObject())
            .withIfNotExists(
                getOrDefault(
                    command.options(),
                    CreateTextIndexCommand.CommandOptions::ifNotExists,
                    TableDescDefaults.CreateTextIndexOptionsDefaults.IF_NOT_EXISTS))
            .withSchemaRetryPolicy(
                new SchemaDBTask.SchemaRetryPolicy(
                    commandContext
                        .config()
                        .get(OperationsConfig.class)
                        .databaseConfig()
                        .ddlRetries(),
                    Duration.ofMillis(
                        commandContext
                            .config()
                            .get(OperationsConfig.class)
                            .databaseConfig()
                            .ddlRetryDelayMillis())));

    // this will throw APIException if the index is not supported
    ApiTextIndex apiIndex =
        ApiTextIndex.FROM_DESC_FACTORY.create(
            commandContext.schemaObject(), indexName, command.definition());
    taskBuilder.withExceptionHandlerFactory(
        DefaultDriverExceptionHandler.Factory.withIdentifier(
            CreateIndexExceptionHandler::new, apiIndex.indexName()));

    var taskGroup = new TaskGroup<>(taskBuilder.build(apiIndex));

    return new TaskOperation<>(
        taskGroup, SchemaDBTaskPage.accumulator(CreateIndexDBTask.class, commandContext));
  }
}
