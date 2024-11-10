package io.stargate.sgv2.jsonapi.service.resolver;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateVectorIndexCommand;
import io.stargate.sgv2.jsonapi.config.DebugModeConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.*;
import io.stargate.sgv2.jsonapi.service.operation.tables.CreateIndexAttemptBuilder;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiVectorIndex;
import io.stargate.sgv2.jsonapi.util.defaults.DefaultBoolean;
import io.stargate.sgv2.jsonapi.util.defaults.Defaults;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Duration;

/** Resolver for the {@link CreateVectorIndexCommand}. */
@ApplicationScoped
public class CreateVectorIndexCommandResolver implements CommandResolver<CreateVectorIndexCommand> {

  // Command option
  private static final DefaultBoolean IF_NOT_EXISTS_DEFAULT = Defaults.of(false);

  @Override
  public Class<CreateVectorIndexCommand> getCommandClass() {
    return CreateVectorIndexCommand.class;
  }

  @Override
  public Operation resolveTableCommand(
      CommandContext<TableSchemaObject> ctx, CreateVectorIndexCommand command) {

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
    var attempt =
        attemptBuilder.build(
            ApiVectorIndex.FROM_DESC_FACTORY.create(
                ctx.schemaObject(), command.name(), command.definition()));

    var pageBuilder =
        SchemaAttemptPage.<TableSchemaObject>builder()
            .debugMode(ctx.getConfig(DebugModeConfig.class).enabled())
            .useErrorObjectV2(ctx.getConfig(OperationsConfig.class).extendError());

    return new GenericOperation<>(
        new OperationAttemptContainer<>(attempt), pageBuilder, new TableDriverExceptionHandler());

    //        // Validate Column present in Table
    //        final Optional<ColumnMetadata> column =
    //            ctx.schemaObject()
    //                .tableMetadata()
    //                .getColumn(CqlIdentifierUtil.cqlIdentifierFromUserInput(columnName));
    //
    //
    //        ColumnMetadata columnMetadata =
    //            column.orElseThrow(
    //                () ->
    //                    SchemaException.Code.INVALID_INDEX_DEFINITION.get(
    //                        Map.of("reason", "Column not defined in the table")));
    //
    //        SimilarityFunction similarityFunction =
    //            definitionOptions != null ? definitionOptions.metric() : null;
    //
    //        String sourceModel = definitionOptions != null ? definitionOptions.sourceModel() :
    // null;
    //
    //        if (!(columnMetadata.getType() instanceof VectorType)) {
    //          throw SchemaException.Code.INVALID_INDEX_DEFINITION.get(
    //              Map.of("reason", "use `createIndex` command to create index on non-vector type
    //     column"));
    //        }
    //
    //        if (definitionOptions != null) {
    //          if (sourceModel != null && SourceModel.getSimilarityFunction(sourceModel) == null) {
    //            throw SchemaException.Code.INVALID_INDEX_DEFINITION.get(
    //                Map.of(
    //                    "reason",
    //                    "sourceModel `%s` used in request is invalid. Supported source models are:
    // %s"
    //                        .formatted(sourceModel, SourceModel.getAllSourceModelNames())));
    //          }
    //        }
    //
    //
    //        // Default Similarity Function to COSINE
    //        if (similarityFunction == null && sourceModel == null) {
    //          similarityFunction = SimilarityFunction.COSINE;
    //        }
    //
    //        final SchemaAttempt.SchemaRetryPolicy schemaRetryPolicy =
    //            new SchemaAttempt.SchemaRetryPolicy(
    //                ctx.getConfig(OperationsConfig.class).databaseConfig().ddlRetries(),
    //                Duration.ofMillis(
    //
    //     ctx.getConfig(OperationsConfig.class).databaseConfig().ddlRetryDelayMillis()));
    //
    //        var attempt =
    //            new CreateIndexAttemptBuilder(
    //                    0, ctx.schemaObject(), columnName, indexName, schemaRetryPolicy)
    //                .ifNotExists(ifNotExists)
    //                .vectorIndexOptions(similarityFunction, sourceModel)
    //                .build();
    //        var attempts = new OperationAttemptContainer<>(List.of(attempt));
    //        var pageBuilder =
    //            SchemaAttemptPage.<TableSchemaObject>builder()
    //                .debugMode(ctx.getConfig(DebugModeConfig.class).enabled())
    //                .useErrorObjectV2(ctx.getConfig(OperationsConfig.class).extendError());
    //
    //        return new GenericOperation<>(attempts, pageBuilder, new
    // TableDriverExceptionHandler());
  }
}
