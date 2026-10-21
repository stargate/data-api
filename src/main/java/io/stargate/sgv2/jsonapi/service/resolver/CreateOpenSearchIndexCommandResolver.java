package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;
import static io.stargate.sgv2.jsonapi.util.ApiOptionUtils.getOrDefault;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateOpenSearchIndexCommand;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.config.constants.TableDescDefaults;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.SchemaDBTask;
import io.stargate.sgv2.jsonapi.service.operation.SchemaDBTaskPage;
import io.stargate.sgv2.jsonapi.service.operation.tables.CreateIndexDBTask;
import io.stargate.sgv2.jsonapi.service.operation.tables.CreateIndexExceptionHandler;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskGroup;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskOperation;
import io.stargate.sgv2.jsonapi.service.schema.naming.NamingRules;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiIndexType;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiOpenSearchIndex;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Duration;

/**
 * Resolver for the {@link CreateOpenSearchIndexCommand}.
 *
 * <p>Validates that no OpenSearch index already exists on the table (only one is permitted), then
 * delegates to {@link CreateIndexDBTask} to emit the {@code CREATE CUSTOM INDEX … USING
 * 'OpenSearchIndex'} CQL statement.
 */
@ApplicationScoped
public class CreateOpenSearchIndexCommandResolver
    implements CommandResolver<CreateOpenSearchIndexCommand> {

  @Override
  public Class<CreateOpenSearchIndexCommand> getCommandClass() {
    return CreateOpenSearchIndexCommand.class;
  }

  @Override
  public Operation<TableSchemaObject> resolveTableCommand(
      CommandContext<TableSchemaObject> commandContext, CreateOpenSearchIndexCommand command) {

    var schemaObject = commandContext.schemaObject();

    // Validate no duplicate OpenSearch index — only one per table is permitted.
    boolean hasExistingOpenSearchIndex =
        schemaObject.apiTableDef().indexes().allIndexes().stream()
            .anyMatch(idx -> idx.indexType() == ApiIndexType.OPEN_SEARCH);
    if (hasExistingOpenSearchIndex) {
      throw SchemaException.Code.OPEN_SEARCH_INDEX_ALREADY_EXISTS.get(errVars(schemaObject));
    }

    var indexName = NamingRules.INDEX.checkRule(command.name());

    var taskBuilder =
        CreateIndexDBTask.builder(schemaObject)
            .withIfNotExists(
                getOrDefault(
                    command.options(),
                    CreateOpenSearchIndexCommand.CommandOptions::ifNotExists,
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

    // Build the ApiOpenSearchIndex from the user-supplied definition descriptor.
    ApiOpenSearchIndex apiIndex =
        ApiOpenSearchIndex.FROM_DESC_FACTORY.create(schemaObject, indexName, command.definition());
    taskBuilder.withExceptionHandlerFactory(
        DefaultDriverExceptionHandler.Factory.withIdentifier(
            CreateIndexExceptionHandler::new, apiIndex.indexName()));

    var taskGroup = new TaskGroup<>(taskBuilder.build(apiIndex));

    return new TaskOperation<>(
        taskGroup, SchemaDBTaskPage.accumulator(CreateIndexDBTask.class, commandContext));
  }
}
