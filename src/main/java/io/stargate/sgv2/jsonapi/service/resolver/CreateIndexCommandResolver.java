package io.stargate.sgv2.jsonapi.service.resolver;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.VectorType;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateIndexCommand;
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

/** Resolver for the {@link CreateIndexCommand}. */
@ApplicationScoped
public class CreateIndexCommandResolver implements CommandResolver<CreateIndexCommand> {
  @Override
  public Class<CreateIndexCommand> getCommandClass() {
    return CreateIndexCommand.class;
  }
  ;

  @Override
  public Operation resolveTableCommand(
      CommandContext<TableSchemaObject> ctx, CreateIndexCommand command) {

    String columnName = command.definition().column();
    String indexName = command.name();
    final CreateIndexCommand.Definition.Options definitionOptions = command.definition().options();

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
    Boolean caseSensitive = definitionOptions != null ? definitionOptions.caseSensitive() : null;
    Boolean normalize = definitionOptions != null ? definitionOptions.normalize() : null;
    Boolean ascii = definitionOptions != null ? definitionOptions.ascii() : null;
    SimilarityFunction similarityFunction =
        definitionOptions != null ? definitionOptions.metric() : null;
    String sourceModel = definitionOptions != null ? definitionOptions.sourceModel() : null;
    if (definitionOptions != null) {
      // Validate Options
      if (!columnMetadata.getType().equals(DataTypes.TEXT)) {
        if (caseSensitive != null || normalize != null || ascii != null) {
          throw SchemaException.Code.INVALID_INDEX_DEFINITION.get(
              Map.of(
                  "reason",
                  "`caseSensitive`, `normalize` and `ascii` options are valid only for `text` column"));
        }
      }
      if (!(columnMetadata.getType() instanceof VectorType)) {
        if (similarityFunction != null || sourceModel != null) {
          throw SchemaException.Code.INVALID_INDEX_DEFINITION.get(
              Map.of(
                  "reason",
                  "`metric` and `sourceModel` options are valid only for `vector` type column"));
        }
      } else {
        if (similarityFunction != null && sourceModel != null) {
          throw SchemaException.Code.INVALID_INDEX_DEFINITION.get(
              Map.of(
                  "reason",
                  "Only one of `metric` or `sourceModel` options should be used for `vector` type column"));
        }
        if (sourceModel != null && !VectorConstant.SUPPORTED_SOURCES.contains(sourceModel)) {
          throw SchemaException.Code.INVALID_INDEX_DEFINITION.get(
              Map.of(
                  "reason",
                  "Invalid `sourceModel`. Supported source models are: "
                      + VectorConstant.SUPPORTED_SOURCES));
        }
      }
    }

    // Command level option for ifNotExists
    boolean ifNotExists = false;
    final CreateIndexCommand.Options commandOptions = command.options();
    if (commandOptions != null && commandOptions.ifNotExists() != null) {
      ifNotExists = commandOptions.ifNotExists();
    }

    // Default Similarity Function to COSINE
    if (columnMetadata.getType() instanceof VectorType
        && similarityFunction == null
        && sourceModel == null) {
      similarityFunction = SimilarityFunction.COSINE;
    }

    var attempt =
        new CreateIndexAttemptBuilder(0, ctx.schemaObject(), columnName, indexName)
            .ifNotExists(ifNotExists)
            .textIndexOptions(caseSensitive, normalize, ascii)
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