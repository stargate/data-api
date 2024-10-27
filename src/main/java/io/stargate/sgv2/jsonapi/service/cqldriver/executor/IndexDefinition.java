package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.type.VectorType;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateIndexCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateVectorIndexCommand;
import io.stargate.sgv2.jsonapi.config.constants.TableIndexConstants;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import java.util.Map;
import java.util.Optional;

/** Definition for an index */
public record IndexDefinition(
    IndexType indexType, String columnName, String indexName, Map<String, String> options) {

  /** Defines what type of index configuration (vector or regular) */
  enum IndexType {
    VECTOR,
    REGULAR
  }

  public static IndexDefinition from(ColumnMetadata columnMetadata, IndexMetadata indexMetadata) {
    final String indexName = CqlIdentifierUtil.externalRepresentation(indexMetadata.getName());
    final String columnName = CqlIdentifierUtil.externalRepresentation(columnMetadata.getName());
    final Map<String, String> options = indexMetadata.getOptions();
    return new IndexDefinition(
        columnMetadata.getType() instanceof VectorType ? IndexType.VECTOR : IndexType.REGULAR,
        columnName,
        indexName,
        options);
  }

  public Object getIndexDefinition() {
    // Logic to return index definition based on index type
    return switch (indexType) {
      case VECTOR -> createVectorIndexDefinition();
      case REGULAR -> createRegularIndexDefinition();
    };
  }

  private CreateVectorIndexCommand createVectorIndexDefinition() {
    CreateVectorIndexCommand.Definition.Options vectorOptions = null;

    final String sourceModel =
        Optional.ofNullable(options.get(TableIndexConstants.IndexOptionKeys.SOURCE_MODEL_OPTION))
            .map((model) -> model.toLowerCase())
            .orElse(null);

    final SimilarityFunction similarityFunction =
        Optional.ofNullable(
                options.get(TableIndexConstants.IndexOptionKeys.SIMILARITY_FUNCTION_OPTION))
            .map((func) -> SimilarityFunction.fromString(func.toLowerCase()))
            .orElse(null);

    if (similarityFunction != null || sourceModel != null) {
      vectorOptions =
          new CreateVectorIndexCommand.Definition.Options(similarityFunction, sourceModel);
    }

    CreateVectorIndexCommand.Definition definition =
        new CreateVectorIndexCommand.Definition(columnName, vectorOptions);
    return new CreateVectorIndexCommand(indexName, definition, null);
  }

  private CreateIndexCommand createRegularIndexDefinition() {
    CreateIndexCommand.Definition.Options indexOptions = null;

    final Boolean caseSensitive =
        Optional.ofNullable(options.get(TableIndexConstants.IndexOptionKeys.CASE_SENSITIVE_OPTION))
            .map(Boolean::valueOf)
            .orElse(null);
    final Boolean normalize =
        Optional.ofNullable(options.get(TableIndexConstants.IndexOptionKeys.NORMALIZE_OPTION))
            .map(Boolean::valueOf)
            .orElse(null);

    final Boolean ascii =
        Optional.ofNullable(options.get(TableIndexConstants.IndexOptionKeys.ASCII_OPTION))
            .map(Boolean::valueOf)
            .orElse(null);

    if (caseSensitive != null || normalize != null || ascii != null) {
      indexOptions = new CreateIndexCommand.Definition.Options(caseSensitive, normalize, ascii);
    }

    CreateIndexCommand.Definition definition =
        new CreateIndexCommand.Definition(columnName, indexOptions);
    return new CreateIndexCommand(indexName, definition, null);
  }
}
