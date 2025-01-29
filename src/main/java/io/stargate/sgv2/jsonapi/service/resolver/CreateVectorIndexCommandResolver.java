package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtJoin;
import static io.stargate.sgv2.jsonapi.util.ApiPropertyUtils.getOrDefault;

import static io.stargate.sgv2.jsonapi.util.NamingValidationUtil.*;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateVectorIndexCommand;
import io.stargate.sgv2.jsonapi.config.DebugModeConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.config.constants.TableDescDefaults;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.*;
import io.stargate.sgv2.jsonapi.service.operation.tables.CreateIndexAttemptBuilder;
import io.stargate.sgv2.jsonapi.service.operation.tables.CreateIndexExceptionHandler;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiIndexType;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiVectorIndex;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Duration;
import java.util.Map;

import static io.stargate.sgv2.jsonapi.util.NamingValidationUtil.*;
import static io.stargate.sgv2.jsonapi.util.NamingValidationUtil.NULL_SCHEMA_NAME;

/** Resolver for the {@link CreateVectorIndexCommand}. */
@ApplicationScoped
public class CreateVectorIndexCommandResolver implements CommandResolver<CreateVectorIndexCommand> {

  @Override
  public Class<CreateVectorIndexCommand> getCommandClass() {
    return CreateVectorIndexCommand.class;
  }

  @Override
  public Operation resolveTableCommand(
      CommandContext<TableSchemaObject> ctx, CreateVectorIndexCommand command) {

      if (!isValidName(command.name())) {
          throw SchemaException.Code.UNSUPPORTED_SCHEMA_NAME.get(
                  Map.of(
                          "schemeType",
                          INDEX_SCHEMA_NAME,
                          "nameLength",
                          String.valueOf(NAME_LENGTH),
                          "unsupportedSchemeName",
                          command.name() == null ? NULL_SCHEMA_NAME : command.name()));
      }

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

    var attemptBuilder = new CreateIndexAttemptBuilder(ctx.schemaObject());

    attemptBuilder =
        attemptBuilder.withIfNotExists(
            getOrDefault(
                command.options(),
                CreateVectorIndexCommand.CreateVectorIndexCommandOptions::ifNotExists,
                TableDescDefaults.CreateVectorIndexOptionsDefaults.IF_NOT_EXISTS));

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
