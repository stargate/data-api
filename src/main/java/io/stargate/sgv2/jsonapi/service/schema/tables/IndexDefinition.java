package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.VectorType;
import io.stargate.sgv2.jsonapi.api.model.command.IndexCreationCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateIndexCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateVectorIndexCommand;
import io.stargate.sgv2.jsonapi.config.constants.TableIndexConstants;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Represents an index definition for a column in a table. */
public class IndexDefinition {
  private final IndexType indexType;
  private final CqlIdentifier columnName;
  private final CqlIdentifier indexName;
  private final Map<String, String> optionsFromDriver;

  /*
   * @param indexType The type of the index (vector or regular).
   * @param columnName The name of the column the index is created on.
   * @param indexName The name of the index.
   * @param options Additional options for the index.
   */

  private IndexDefinition(
      IndexType indexType,
      CqlIdentifier columnName,
      CqlIdentifier indexName,
      Map<String, String> optionsFromDriver) {
    this.indexType = indexType;
    this.columnName = columnName;
    this.indexName = indexName;
    this.optionsFromDriver = optionsFromDriver;
  }

  /** Defines what type of index configuration (vector or regular) */
  public enum IndexType {
    VECTOR,
    REGULAR;

    public static IndexType fromCqlDataType(DataType cqlDataType) {
      return cqlDataType instanceof VectorType ? VECTOR : REGULAR;
    }
  }

  public static IndexDefinition from(ColumnMetadata columnMetadata, IndexMetadata indexMetadata) {
    Objects.requireNonNull(columnMetadata, "Column metadata cannot be null");
    Objects.requireNonNull(indexMetadata, "Index metadata cannot be null");
    final Map<String, String> optionsFromDriver = indexMetadata.getOptions();
    return new IndexDefinition(
        IndexType.fromCqlDataType(columnMetadata.getType()),
        columnMetadata.getName(),
        indexMetadata.getName(),
        optionsFromDriver);
  }

  public IndexCreationCommand getIndexDefinition() {
    // Logic to return index definition based on index type
    return switch (indexType) {
      case VECTOR -> createVectorIndexDefinition();
      case REGULAR -> createRegularIndexDefinition();
    };
  }

  private CreateVectorIndexCommand createVectorIndexDefinition() {
    final String sourceModel =
        Optional.ofNullable(
                optionsFromDriver.get(TableIndexConstants.IndexOptionKeys.SOURCE_MODEL_OPTION))
            .map(String::toLowerCase)
            .orElse(null);

    final SimilarityFunction similarityFunction =
        Optional.ofNullable(
                optionsFromDriver.get(
                    TableIndexConstants.IndexOptionKeys.SIMILARITY_FUNCTION_OPTION))
            .map((func) -> SimilarityFunction.fromString(func.toLowerCase()))
            .orElse(null);

    CreateVectorIndexCommand.Definition.Options vectorOptions = null;
    if (similarityFunction != null || sourceModel != null) {
      vectorOptions =
          new CreateVectorIndexCommand.Definition.Options(similarityFunction, sourceModel);
    }

    CreateVectorIndexCommand.Definition definition =
        new CreateVectorIndexCommand.Definition(
            CqlIdentifierUtil.externalRepresentation(columnName), vectorOptions);
    return new CreateVectorIndexCommand(
        CqlIdentifierUtil.externalRepresentation(indexName), definition, null);
  }

  private CreateIndexCommand createRegularIndexDefinition() {
    final Boolean caseSensitive =
        Optional.ofNullable(
                optionsFromDriver.get(TableIndexConstants.IndexOptionKeys.CASE_SENSITIVE_OPTION))
            .map(Boolean::valueOf)
            .orElse(TableIndexConstants.IndexOptionDefault.CASE_SENSITIVE_OPTION_DEFAULT);
    final Boolean normalize =
        Optional.ofNullable(
                optionsFromDriver.get(TableIndexConstants.IndexOptionKeys.NORMALIZE_OPTION))
            .map(Boolean::valueOf)
            .orElse(TableIndexConstants.IndexOptionDefault.NORMALIZE_OPTION_DEFAULT);

    final Boolean ascii =
        Optional.ofNullable(optionsFromDriver.get(TableIndexConstants.IndexOptionKeys.ASCII_OPTION))
            .map(Boolean::valueOf)
            .orElse(TableIndexConstants.IndexOptionDefault.ASCII_OPTION_DEFAULT);

    CreateIndexCommand.Definition.Options indexOptions =
        new CreateIndexCommand.Definition.Options(caseSensitive, normalize, ascii);
    CreateIndexCommand.Definition definition =
        new CreateIndexCommand.Definition(
            CqlIdentifierUtil.externalRepresentation(columnName), indexOptions);
    return new CreateIndexCommand(
        CqlIdentifierUtil.externalRepresentation(indexName), definition, null);
  }

  public CqlIdentifier getColumnName() {
    return columnName;
  }

  public CqlIdentifier getIndexName() {
    return indexName;
  }

  public String getOption(String optionName) {
    return optionsFromDriver.get(optionName);
  }
}
