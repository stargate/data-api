package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DropIndexCommand;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableExtensions;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.SchemaDBTask;
import io.stargate.sgv2.jsonapi.service.operation.SchemaDBTaskPage;
import io.stargate.sgv2.jsonapi.service.operation.keyspaces.KeyspaceDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.operation.tables.DropIndexDBTask;
import io.stargate.sgv2.jsonapi.service.operation.tables.DropIndexExceptionHandler;
import io.stargate.sgv2.jsonapi.service.operation.tables.DropVectorIndexProfileDBTask;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskGroup;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskOperation;
import io.stargate.sgv2.jsonapi.service.schema.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.util.ApiOptionUtils;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;

/** Resolver for the {@link DropIndexCommand}. */
@ApplicationScoped
public class DropIndexCommandResolver implements CommandResolver<DropIndexCommand> {

  private static final boolean IF_EXISTS_DEFAULT = false;

  @Inject ObjectMapper objectMapper;

  @Override
  public Class<DropIndexCommand> getCommandClass() {
    return DropIndexCommand.class;
  }

  @Override
  public Operation<KeyspaceSchemaObject> resolveKeyspaceCommand(
      CommandContext<KeyspaceSchemaObject> commandContext, DropIndexCommand command) {

    var schemaObject = commandContext.schemaObject();
    var indexName = cqlIdentifierFromUserInput(command.name());
    // Check if the index exists, we check if columns exist before trying to drop them so do for
    // indexes as well

    var schemaRetryPolicy =
        new SchemaDBTask.SchemaRetryPolicy(
            commandContext.config().get(OperationsConfig.class).databaseConfig().ddlRetries(),
            Duration.ofMillis(
                commandContext
                    .config()
                    .get(OperationsConfig.class)
                    .databaseConfig()
                    .ddlRetryDelayMillis()));

    var dropIndexTask =
        DropIndexDBTask.builder(schemaObject)
            .withSchemaRetryPolicy(schemaRetryPolicy)
            .withExceptionHandlerFactory(
                DefaultDriverExceptionHandler.Factory.withIdentifier(
                    DropIndexExceptionHandler::new, indexName))
            .withIndexName(indexName)
            .withIfExists(
                ApiOptionUtils.getOrDefault(
                    command.options(), DropIndexCommand.Options::ifExists, IF_EXISTS_DEFAULT))
            .build();

    // Also drop the index's vector-index profile (if any) from the owning table's extensions, so
    // the profile record does not outlive the index. Null when the keyspace metadata is unknown or
    // the owning table has no stored profile for this index, in which case only the drop runs.
    var profileCleanupTask = buildProfileCleanupTask(schemaObject, indexName, schemaRetryPolicy);

    if (profileCleanupTask == null) {
      return new TaskOperation<>(
          new TaskGroup<>(dropIndexTask),
          SchemaDBTaskPage.accumulator(DropIndexDBTask.class, commandContext));
    }

    // Sequential so the extension cleanup only runs if the index drop succeeded.
    TaskGroup<SchemaDBTask<KeyspaceSchemaObject>, KeyspaceSchemaObject> taskGroup =
        new TaskGroup<>(true);
    taskGroup.add(dropIndexTask);
    taskGroup.add(profileCleanupTask);

    @SuppressWarnings("unchecked")
    Class<SchemaDBTask<KeyspaceSchemaObject>> taskClass =
        (Class<SchemaDBTask<KeyspaceSchemaObject>>) (Class<?>) SchemaDBTask.class;
    return new TaskOperation<>(taskGroup, SchemaDBTaskPage.accumulator(taskClass, commandContext));
  }

  /**
   * Builds the cleanup task that removes the dropped index's profile from its owning table's
   * extensions, or null when there is nothing to clean up (keyspace metadata unknown, no owning
   * table, or no stored profile for this index).
   */
  private DropVectorIndexProfileDBTask buildProfileCleanupTask(
      KeyspaceSchemaObject schemaObject,
      CqlIdentifier indexName,
      SchemaDBTask.SchemaRetryPolicy schemaRetryPolicy) {

    return schemaObject
        .keyspaceMetadata()
        .flatMap(
            keyspaceMetadata ->
                TableExtensions.removeIndexProfile(keyspaceMetadata, indexName, objectMapper))
        .map(
            removal ->
                DropVectorIndexProfileDBTask.builder(schemaObject)
                    .withSchemaRetryPolicy(schemaRetryPolicy)
                    .withExceptionHandlerFactory(KeyspaceDriverExceptionHandler::new)
                    .withTableName(removal.tableName())
                    .withCustomProperties(removal.customProperties())
                    .build())
        .orElse(null);
  }
}
