package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtJoin;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateTableCommand;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.*;
import io.stargate.sgv2.jsonapi.service.operation.*;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.tables.CreateTableDBTask;
import io.stargate.sgv2.jsonapi.service.operation.tables.CreateTableExceptionHandler;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskGroup;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskOperation;
import io.stargate.sgv2.jsonapi.service.schema.naming.NamingRules;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataTypeDefs;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiSupportDef;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTableDef;
import io.stargate.sgv2.jsonapi.util.ApiOptionUtils;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class CreateTableCommandResolver implements CommandResolver<CreateTableCommand> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CreateTableCommandResolver.class);

  @Inject ObjectMapper objectMapper;
  @Inject VectorizeConfigValidator validateVectorize;

  @Override
  public Operation<KeyspaceSchemaObject> resolveKeyspaceCommand(
      CommandContext<KeyspaceSchemaObject> commandContext, CreateTableCommand command) {

    var taskBuilder =
        CreateTableDBTask.builder(commandContext.schemaObject())
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

    // TODO: XXX: AARON: Validation should happen in the factory for the table
    var tableName = NamingRules.TABLE.checkRule(command.name());

    taskBuilder.ifNotExists(
        ApiOptionUtils.getOrDefault(
            command.options(), CreateTableCommand.Options::ifNotExists, false));

    var apiTableDef =
        ApiTableDef.FROM_TABLE_DESC_FACTORY.create(
            tableName, command.definition(), validateVectorize);

    var unsupportedColumnsCreateTable =
        apiTableDef.allColumns().filterBySupportToList(x -> !x.createTable());
    if (!unsupportedColumnsCreateTable.isEmpty()) {
      throw SchemaException.Code.UNSUPPORTED_DATA_TYPE_TABLE_CREATION.get(
          Map.of(
              "supportedTypes",
                  // Notice, supported map/set/list types are not included in the error message
                  // They will be validated before in the desc factory
                  errFmtJoin(
                      ApiDataTypeDefs.filterBySupportToList(ApiSupportDef::createTable).stream()
                          .map(e -> e.typeName().apiName())
                          .sorted(String::compareTo)
                          .toList()),
              "unsupportedTypes",
                  errFmtJoin(
                      unsupportedColumnsCreateTable.stream()
                          .map(e -> e.type().apiName())
                          .sorted(String::compareTo)
                          .toList())));
    }

    // todo - move the custom properties building into the builder
    taskBuilder
        .tableDef(apiTableDef)
        .customProperties(
            TableExtensions.createCustomProperties(
                apiTableDef.allColumns().getVectorizeDefs(), objectMapper))
        .withExceptionHandlerFactory(
            DefaultDriverExceptionHandler.Factory.withIdentifier(
                CreateTableExceptionHandler::new, cqlIdentifierFromUserInput(tableName)));

    var taskGroup = new TaskGroup<>(taskBuilder.build());

    return new TaskOperation<>(
        taskGroup, SchemaDBTaskPage.accumulator(CreateTableDBTask.class, commandContext));
  }

  @Override
  public Class<CreateTableCommand> getCommandClass() {
    return CreateTableCommand.class;
  }
}
