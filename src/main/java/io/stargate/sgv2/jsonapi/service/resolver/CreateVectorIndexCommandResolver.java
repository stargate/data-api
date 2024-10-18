package io.stargate.sgv2.jsonapi.service.resolver;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.VectorType;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateVectorIndexCommand;
import io.stargate.sgv2.jsonapi.config.DebugModeConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.config.constants.VectorConstant;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.GenericOperation;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.OperationAttemptContainer;
import io.stargate.sgv2.jsonapi.service.operation.SchemaAttemptPage;
import io.stargate.sgv2.jsonapi.service.operation.tables.CreateIndexAttemptBuilder;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Resolver for the {@link CreateVectorIndexCommand}. */
@ApplicationScoped
public class CreateVectorIndexCommandResolver implements CommandResolver<CreateVectorIndexCommand> {
  @Override
  public Class<CreateVectorIndexCommand> getCommandClass() {
    return CreateVectorIndexCommand.class;
  }
  ;

  @Override
  public Operation resolveTableCommand(
      CommandContext<TableSchemaObject> ctx, CreateVectorIndexCommand command) {

    String columnName = command.definition().column();
    String indexName = command.name();
    final CreateVectorIndexCommand.Definition.Options definitionOptions =
        command.definition().options();

    TableMetadata tableMetadata = ctx.schemaObject().tableMetadata();
    // Validate Column present in Table
    final Optional<ColumnMetadata> column =
        ctx.schemaObject()
            .tableMetadata()
            .getColumn(CqlIdentifierUtil.cqlIdentifierFromUserInput(columnName));
    ColumnMetadata columnMetadata =
        column.orElseThrow(
            () ->
                SchemaException.Code.INVALID_INDEX_DEFINITION.get(
                    Map.of("reason", "Column not defined in the table")));
    SimilarityFunction similarityFunction =
        definitionOptions != null ? definitionOptions.metric() : null;
    String sourceModel = definitionOptions != null ? definitionOptions.sourceModel() : null;
    if (!(columnMetadata.getType() instanceof VectorType)) {
      throw SchemaException.Code.INVALID_INDEX_DEFINITION.get(
          Map.of("reason", "use `createIndex` command to create index on non-vector type column"));
    }

    if (definitionOptions != null) {
      if (sourceModel != null && VectorConstant.SUPPORTED_SOURCES.get(sourceModel) == null) {
        throw SchemaException.Code.INVALID_INDEX_DEFINITION.get(
            Map.of(
                "reason",
                "sourceModel `%s` used is request is invalid. Supported source models are: %s"
                    .formatted(sourceModel, VectorConstant.SUPPORTED_SOURCES.keySet())));
      }
    }

    // Command level option for ifNotExists
    boolean ifNotExists = false;
    final CreateVectorIndexCommand.Options commandOptions = command.options();
    if (commandOptions != null && commandOptions.ifNotExists() != null) {
      ifNotExists = commandOptions.ifNotExists();
    }

    // Default Similarity Function to COSINE
    if (similarityFunction == null && sourceModel == null) {
      similarityFunction = SimilarityFunction.COSINE;
    }

    var attempt =
        new CreateIndexAttemptBuilder(0, ctx.schemaObject(), columnName, indexName)
            .ifNotExists(ifNotExists)
            .vectorIndexOptions(similarityFunction, sourceModel)
            .build();
    var attempts = new OperationAttemptContainer<>(List.of(attempt));
    var pageBuilder =
        SchemaAttemptPage.<TableSchemaObject>builder()
            .debugMode(ctx.getConfig(DebugModeConfig.class).enabled())
            .useErrorObjectV2(ctx.getConfig(OperationsConfig.class).extendError());

    return new GenericOperation<>(attempts, pageBuilder, new TableDriverExceptionHandler());
  }
}
