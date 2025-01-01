package io.stargate.sgv2.jsonapi.service.resolver;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateVectorIndexCommand;
import io.stargate.sgv2.jsonapi.config.DebugModeConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.*;
import io.stargate.sgv2.jsonapi.service.operation.tables.CreateIndexAttemptBuilder;
import io.stargate.sgv2.jsonapi.service.operation.tables.CreateIndexExceptionHandler;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiIndexType;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiVectorIndex;
import io.stargate.sgv2.jsonapi.util.defaults.DefaultBoolean;
import io.stargate.sgv2.jsonapi.util.defaults.Defaults;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Duration;
import java.util.Map;

/** Resolver for the {@link CreateVectorIndexCommand}. */
@ApplicationScoped
public class CreateVectorIndexCommandResolver implements CommandResolver<CreateVectorIndexCommand> {

  // Command option
  public static final DefaultBoolean IF_NOT_EXISTS_DEFAULT = Defaults.of(false);

  @Override
  public Class<CreateVectorIndexCommand> getCommandClass() {
    return CreateVectorIndexCommand.class;
  }

  @Override
  public Operation resolveTableCommand(
      CommandContext<TableSchemaObject> ctx, CreateVectorIndexCommand command) {

    ApiIndexType indexType =
        command.indexType() == null
            ? ApiIndexType.VECTOR
            : ApiIndexType.fromTypeName(command.indexType());

    if (indexType == null) {
      throw SchemaException.Code.UNKNOWN_INDEX_TYPES.get(
          Map.of(
              "supportedTypes", ApiIndexType.all().toString(), "unknownType", command.indexType()));
    }

    if (indexType != ApiIndexType.VECTOR) {
      throw SchemaException.Code.UNSUPPORTED_INDEX_TYPES.get(
          Map.of(
              "supportedTypes",
              ApiIndexType.VECTOR.toString(),
              "unsupportedType",
              command.indexType()));
    }

    var attemptBuilder = new CreateIndexAttemptBuilder(ctx.schemaObject());

    attemptBuilder =
        attemptBuilder.withIfNotExists(
            IF_NOT_EXISTS_DEFAULT.apply(
                command.options(),
                CreateVectorIndexCommand.CreateVectorIndexCommandOptions::ifNotExists));

    // TODO: we need a centralised way of creating retry attempt.
    attemptBuilder =
        attemptBuilder.withSchemaRetryPolicy(
            new SchemaAttempt.SchemaRetryPolicy(
                ctx.getConfig(OperationsConfig.class).databaseConfig().ddlRetries(),
                Duration.ofMillis(
                    ctx.getConfig(OperationsConfig.class).databaseConfig().ddlRetryDelayMillis())));

    // this will throw APIException if the index is not supported
    var apiIndex =
        ApiVectorIndex.FROM_DESC_FACTORY.create(
            ctx.schemaObject(), command.name(), command.definition());
    var attempt = attemptBuilder.build(apiIndex);

    var pageBuilder =
        SchemaAttemptPage.<TableSchemaObject>builder()
            .debugMode(ctx.getConfig(DebugModeConfig.class).enabled())
            .useErrorObjectV2(ctx.getConfig(OperationsConfig.class).extendError());

    return new GenericOperation<>(
        new OperationAttemptContainer<>(attempt),
        pageBuilder,
        new CreateIndexExceptionHandler(apiIndex.indexName()));
  }
}
