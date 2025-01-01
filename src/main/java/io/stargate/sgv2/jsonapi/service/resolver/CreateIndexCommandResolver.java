package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.*;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateIndexCommand;
import io.stargate.sgv2.jsonapi.config.DebugModeConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.GenericOperation;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.OperationAttemptContainer;
import io.stargate.sgv2.jsonapi.service.operation.SchemaAttempt;
import io.stargate.sgv2.jsonapi.service.operation.SchemaAttemptPage;
import io.stargate.sgv2.jsonapi.service.operation.tables.CreateIndexAttemptBuilder;
import io.stargate.sgv2.jsonapi.service.operation.tables.CreateIndexExceptionHandler;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiIndexType;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiRegularIndex;
import io.stargate.sgv2.jsonapi.util.defaults.DefaultBoolean;
import io.stargate.sgv2.jsonapi.util.defaults.Defaults;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Duration;
import java.util.Map;

/** Resolver for the {@link CreateIndexCommand}. */
@ApplicationScoped
public class CreateIndexCommandResolver implements CommandResolver<CreateIndexCommand> {

  // Command option
  public static final DefaultBoolean IF_NOT_EXISTS_DEFAULT = Defaults.of(false);

  @Override
  public Class<CreateIndexCommand> getCommandClass() {
    return CreateIndexCommand.class;
  }

  @Override
  public Operation resolveTableCommand(
      CommandContext<TableSchemaObject> ctx, CreateIndexCommand command) {

    ApiIndexType indexType =
        command.indexType() == null
            ? ApiIndexType.REGULAR
            : ApiIndexType.fromTypeName(command.indexType());

    if (indexType == null) {
      // UNKNOWN_INDEX_TYPES
      throw SchemaException.Code.UNKNOWN_INDEX_TYPES.get(
          Map.of(
              "supportedTypes", ApiIndexType.all().toString(), "unknownType", command.indexType()));
    }

    if (indexType != ApiIndexType.REGULAR) {
      throw SchemaException.Code.UNSUPPORTED_INDEX_TYPES.get(
          Map.of(
              "supportedTypes",
              ApiIndexType.REGULAR.toString(),
              "unsupportedType",
              command.indexType()));
    }

    var attemptBuilder = new CreateIndexAttemptBuilder(ctx.schemaObject());

    attemptBuilder =
        attemptBuilder.withIfNotExists(
            IF_NOT_EXISTS_DEFAULT.apply(
                command.options(), CreateIndexCommand.CreateIndexCommandOptions::ifNotExists));

    // TODO: we need a centralised way of creating retry attempt.
    attemptBuilder =
        attemptBuilder.withSchemaRetryPolicy(
            new SchemaAttempt.SchemaRetryPolicy(
                ctx.getConfig(OperationsConfig.class).databaseConfig().ddlRetries(),
                Duration.ofMillis(
                    ctx.getConfig(OperationsConfig.class).databaseConfig().ddlRetryDelayMillis())));

    // this will throw APIException if the index is not supported
    var apiIndex =
        ApiRegularIndex.FROM_DESC_FACTORY.create(
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
