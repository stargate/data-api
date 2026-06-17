package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtJoin;
import static io.stargate.sgv2.jsonapi.util.ApiOptionUtils.getOrDefault;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToJsonKey;

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

    // If a named profile was used, record the name + the options it resolved to in the table
    // extensions so the friendly name survives. Written as a second DDL after the index so a failed
    // create leaves no orphan record. Returns null (no extension write) when there is nothing to
    // persist or the index already exists (the CREATE IF NOT EXISTS would be a no-op).
    var extensionTask =
        buildProfileExtensionTask(schemaObject, apiIndex, command, schemaRetryPolicy);
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
   * when nothing needs to change. Returns null when the index already exists (a {@code CREATE ...
   * IF NOT EXISTS} would be a no-op, so its stored profile must not be rewritten), or when no
   * profile is used and there is no stale entry to clear. The snapshot stores the options actually
   * applied to the index (profile expansion plus any explicit overrides); existing vectorize config
   * and other profiles are read back and rewritten so they are not lost.
   */
  private AlterTableDBTask buildProfileExtensionTask(
      TableSchemaObject schemaObject,
      ApiVectorIndex apiIndex,
      CreateVectorIndexCommand command,
      SchemaDBTask.SchemaRetryPolicy schemaRetryPolicy) {

    // The create is "IF NOT EXISTS": if the index already exists the create is a no-op, so leave
    // its stored profile untouched (it must keep matching the live index options).
    if (schemaObject.tableMetadata().getIndexes().containsKey(apiIndex.indexName())) {
      return null;
    }

    var options = command.definition().options();
    var vectorIndexing = (options == null) ? null : options.vectorIndexing();
    // Only a named profile is recorded; bare options carry no name to store.
    var profileName = (vectorIndexing == null) ? null : vectorIndexing.profile();

    var indexKey = cqlIdentifierToJsonKey(apiIndex.indexName());
    var profiles = VectorIndexProfileDefinition.from(schemaObject.tableMetadata(), objectMapper);

    // Snapshot the options actually applied to the index (profile expansion plus explicit
    // overrides), so the stored metadata matches the live index rather than the base profile.
    var def =
        (profileName == null)
            ? null
            : new VectorIndexProfileDefinition(profileName, apiIndex.appliedTuningOptions());

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
