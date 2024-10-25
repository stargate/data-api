package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.VectorType;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateIndexCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateVectorIndexCommand;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** Definition of index config */
public class IndexConfig {
  private Map<String, IndexDefinition> indexDefinitions;

  private IndexConfig(Map<String, IndexDefinition> indexDefinitions) {
    this.indexDefinitions = indexDefinitions;
  }

  public static IndexConfig from(TableMetadata tableMetadata) {
    Map<String, IndexDefinition> indexDefinitions =
        tableMetadata.getIndexes().values().stream()
            .map(
                indexMetadata -> {
                  final ColumnMetadata columnMetadata =
                      tableMetadata
                          .getColumn(
                              CqlIdentifierUtil.cqlIdentifierFromUserInput(
                                  indexMetadata.getTarget()))
                          .orElseThrow(
                              () ->
                                  new IllegalArgumentException(
                                      "Column not found for index: " + indexMetadata.getTarget()));
                  return IndexDefinition.from(columnMetadata, indexMetadata);
                })
            .collect(
                Collectors.toMap(IndexDefinition::columnName, indexDefinition -> indexDefinition));
    return new IndexConfig(indexDefinitions);
  }

  /** Get index definition by column name */
  public Optional<IndexDefinition> getIndexDefinition(String columnName) {
    return Optional.ofNullable(indexDefinitions.get(columnName));
  }

  /** Definition for an index */
  public record IndexDefinition(
      IndexType indexType, String columnName, String indexName, Map<String, String> options) {
    public static IndexDefinition from(ColumnMetadata columnMetadata, IndexMetadata indexMetadata) {
      final String indexName =
          CqlIdentifierUtil.cqlIdentifierToStringForUser(indexMetadata.getName());
      final String columnName =
          CqlIdentifierUtil.cqlIdentifierToStringForUser(columnMetadata.getName());
      final Map<String, String> options = indexMetadata.getOptions();
      return new IndexDefinition(
          columnMetadata.getType() instanceof VectorType ? IndexType.VECTOR : IndexType.REGULAR,
          columnName,
          indexName,
          options);
    }

    enum IndexType {
      VECTOR,
      REGULAR
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
          options.get("source_model") != null ? options.get("source_model").toLowerCase() : null;
      final String similarityFunctionValue = options.get("similarity_function");
      SimilarityFunction similarityFunction =
          similarityFunctionValue != null
              ? SimilarityFunction.fromString(similarityFunctionValue.toLowerCase())
              : null;
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
          options.get("case_sensitive") != null
              ? Boolean.valueOf(options.get("case_sensitive"))
              : null;
      final Boolean normalize =
          options.get("normalize") != null ? Boolean.valueOf(options.get("normalize")) : null;
      final Boolean ascii =
          options.get("ascii") != null ? Boolean.valueOf(options.get("ascii")) : null;
      if (caseSensitive != null || normalize != null || ascii != null) {
        indexOptions = new CreateIndexCommand.Definition.Options(caseSensitive, normalize, ascii);
      }

      CreateIndexCommand.Definition definition =
          new CreateIndexCommand.Definition(columnName, indexOptions);
      return new CreateIndexCommand(indexName, definition, null);
    }
  }

  public Map<String, IndexDefinition> indexDefinitions() {
    return indexDefinitions;
  }
}
