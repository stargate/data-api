package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.VectorType;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateIndexCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateVectorIndexCommand;
import io.stargate.sgv2.jsonapi.config.constants.TableIndexConstants;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Definition of index config */
public class IndexConfig {
  /**
   * Map of index definitions where the key is the column name and the value is the index
   * definition.
   */
  private final Map<String, IndexDefinition> indexDefinitions;

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
                                  getColumnName(indexMetadata.getTarget())))
                          .orElseThrow(
                              () ->
                                  new IllegalArgumentException(
                                      "Column not found for index: "
                                          + getColumnName(indexMetadata.getTarget())));
                  return IndexDefinition.from(columnMetadata, indexMetadata);
                })
            .collect(
                Collectors.toMap(IndexDefinition::columnName, indexDefinition -> indexDefinition));
    return new IndexConfig(indexDefinitions);
  }

  private static String getColumnName(String target) {
    Pattern pattern = Pattern.compile("\\(([^)]+)\\)");
    Matcher matcher = pattern.matcher(target);
    if (matcher.find()) {
      return matcher.group(1); // Get the content inside the parentheses
    } else {
      return target;
    }
  }

  /** Get index definition by column name */
  public Optional<IndexDefinition> getIndexDefinition(String columnName) {
    return Optional.ofNullable(indexDefinitions.get(columnName));
  }

  /** Definition for an index */
  public record IndexDefinition(
      IndexType indexType, String columnName, String indexName, Map<String, String> options) {
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
          Optional.ofNullable(
                  options.get(TableIndexConstants.IndexOptionKeys.CASE_SENSITIVE_OPTION))
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

  public Map<String, IndexDefinition> indexDefinitions() {
    return indexDefinitions;
  }
}
