package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtJoin;
import static io.stargate.sgv2.jsonapi.util.ApiOptionUtils.getOrDefault;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToJsonKey;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateVectorIndexCommand;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.config.constants.TableDescDefaults;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableExtensions;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorIndexProfileDefinition;
import io.stargate.sgv2.jsonapi.service.operation.*;
import io.stargate.sgv2.jsonapi.service.operation.tables.AlterTableDBTask;
import io.stargate.sgv2.jsonapi.service.operation.tables.AlterTableExceptionHandler;
import io.stargate.sgv2.jsonapi.service.operation.tables.CreateIndexDBTask;
import io.stargate.sgv2.jsonapi.service.operation.tables.CreateIndexDBTaskBuilder;
import io.stargate.sgv2.jsonapi.service.operation.tables.CreateIndexExceptionHandler;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskGroup;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskOperation;
import io.stargate.sgv2.jsonapi.service.schema.naming.NamingRules;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiIndexType;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiVectorIndex;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.tables.VectorIndexProfiles;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.Map;

/** Resolver for the {@link CreateVectorIndexCommand}. */
@ApplicationScoped
public class CreateVectorIndexCommandResolver implements CommandResolver<CreateVectorIndexCommand> {

  @Inject ObjectMapper objectMapper;

  @Override
  public Class<CreateVectorIndexCommand> getCommandClass() {
    return CreateVectorIndexCommand.class;
  }

  @Override
  public Operation<TableSchemaObject> resolveTableCommand(
      CommandContext<TableSchemaObject> commandContext, CreateVectorIndexCommand command) {

    // TODO: AARON: Validation should happen in the factory for the index
    var indexName = NamingRules.INDEX.checkRule(command.name());

    var indexType =
        command.indexType() == null
            ? ApiIndexType.VECTOR
            : ApiIndexType.fromApiName(command.indexType());

    if (indexType == null) {
      throw SchemaException.Code.UNKNOWN_INDEX_TYPE.get(
          Map.of(
              "knownTypes",
              errFmtJoin(ApiIndexType.values(), ApiIndexType::apiName),
              "unknownType",
              command.indexType()));
    }

    if (indexType != ApiIndexType.VECTOR) {
      throw SchemaException.Code.UNSUPPORTED_INDEX_TYPE.get(
          Map.of(
              "supportedTypes",
              ApiIndexType.VECTOR.apiName(),
              "unsupportedType",
              command.indexType()));
    }

    var schemaObject = commandContext.schemaObject();

    // TODO: we need a centralised way of creating retry attempt.
    var schemaRetryPolicy =
        new SchemaDBTask.SchemaRetryPolicy(
            commandContext.config().get(OperationsConfig.class).databaseConfig().ddlRetries(),
            Duration.ofMillis(
                commandContext
                    .config()
                    .get(OperationsConfig.class)
                    .databaseConfig()
                    .ddlRetryDelayMillis()));

    CreateIndexDBTaskBuilder taskBuilder =
        CreateIndexDBTask.builder(schemaObject)
            .withIfNotExists(
                getOrDefault(
                    command.options(),
                    CreateVectorIndexCommand.CreateVectorIndexCommandOptions::ifNotExists,
                    TableDescDefaults.CreateVectorIndexOptionsDefaults.IF_NOT_EXISTS))
            .withSchemaRetryPolicy(schemaRetryPolicy);

    // this will throw APIException if the index is not supported
    var apiIndex =
        ApiVectorIndex.FROM_DESC_FACTORY.create(schemaObject, indexName, command.definition());
    taskBuilder.withExceptionHandlerFactory(
        DefaultDriverExceptionHandler.Factory.withIdentifier(
            CreateIndexExceptionHandler::new, apiIndex.indexName()));

    var createIndexTask = taskBuilder.build(apiIndex);

    // If a named profile was used, record the name + the options it expanded to in the table
    // extensions so the friendly name survives. Written as a second DDL after the index so a failed
    // create leaves no orphan record. (With ifNotExists on an existing index the create is a no-op
    // but we still write the latest requested profile.)
    var extensionTask =
        buildProfileExtensionTask(schemaObject, apiIndex.indexName(), command, schemaRetryPolicy);
    if (extensionTask == null) {
      return new TaskOperation<>(
          new TaskGroup<>(createIndexTask),
          SchemaDBTaskPage.accumulator(CreateIndexDBTask.class, commandContext));
    }

    // sequential so the extension write only runs if the index was created
    TaskGroup<SchemaDBTask<TableSchemaObject>, TableSchemaObject> taskGroup = new TaskGroup<>(true);
    taskGroup.add(createIndexTask);
    taskGroup.add(extensionTask);

    @SuppressWarnings("unchecked")
    Class<SchemaDBTask<TableSchemaObject>> taskClass =
        (Class<SchemaDBTask<TableSchemaObject>>) (Class<?>) SchemaDBTask.class;
    return new TaskOperation<>(taskGroup, SchemaDBTaskPage.accumulator(taskClass, commandContext));
  }

  /**
   * Builds the ALTER TABLE task that records this index's profile in the table extensions, or null
   * when nothing needs to change (no profile used and no stale entry for the index name to clear).
   * Existing vectorize config and other profiles are read back and rewritten so they are not lost.
   */
  private AlterTableDBTask buildProfileExtensionTask(
      TableSchemaObject schemaObject,
      CqlIdentifier indexIdentifier,
      CreateVectorIndexCommand command,
      SchemaDBTask.SchemaRetryPolicy schemaRetryPolicy) {

    var options = command.definition().options();
    var vectorIndexing = (options == null) ? null : options.vectorIndexing();
    // Only a named profile is recorded; bare options carry no name to store.
    var profileName = (vectorIndexing == null) ? null : vectorIndexing.profile();

    var indexKey = cqlIdentifierToJsonKey(indexIdentifier);
    var profiles = VectorIndexProfileDefinition.from(schemaObject.tableMetadata(), objectMapper);

    VectorIndexProfileDefinition def = null;
    if (profileName != null) {
      // forName was already validated by the index factory above, so it is present here.
      var profileOptions = VectorIndexProfiles.forName(profileName).orElseThrow();
      def = new VectorIndexProfileDefinition(profileName, profileOptions);
    }

    if (!VectorIndexProfileDefinition.putOrRemove(profiles, indexKey, def)) {
      return null;
    }

    var customProperties =
        TableExtensions.createCustomProperties(
            schemaObject.apiTableDef().allColumns().getVectorizeDefs(), profiles, objectMapper);

    return AlterTableDBTask.builder(schemaObject)
        .withRetryPolicy(schemaRetryPolicy)
        .withExceptionHandlerFactory(
            DefaultDriverExceptionHandler.Factory.withIdentifier(
                AlterTableExceptionHandler::new, schemaObject.tableName()))
        .buildUpdateExtensions(customProperties);
  }
}
