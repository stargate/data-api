package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.type.DataType;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;

/** Builder for a {@link CreateIndexAttempt}. */
public class CreateIndexAttemptBuilder {
  private int position;
  private TableSchemaObject schemaObject;
  private String columnName;
  private DataType dataType;
  private String indexName;
  private CreateIndexAttempt.TextIndexOptions textIndexOptions;
  private CreateIndexAttempt.VectorIndexOptions vectorIndexOptions;
  private boolean ifNotExists;

  public CreateIndexAttemptBuilder(int position, TableSchemaObject schemaObject) {
    this.position = position;
    this.schemaObject = schemaObject;
  }

  /*
   * Column name for which index is to be created.
   */
  public CreateIndexAttemptBuilder columnName(String columnName) {
    this.columnName = columnName;
    // It will not be null since null check is already done in the resolver
    this.dataType =
        schemaObject
            .tableMetadata()
            .getColumn(CqlIdentifierUtil.cqlIdentifierFromUserInput(columnName))
            .get()
            .getType();
    return this;
  }

  /*
   * Column name for which index is to be created.
   */
  public CreateIndexAttemptBuilder dataType(DataType dataType) {
    this.dataType = dataType;
    return this;
  }

  public CreateIndexAttemptBuilder indexName(String indexName) {
    this.indexName = indexName;
    return this;
  }

  public CreateIndexAttemptBuilder ifNotExists(boolean ifNotExists) {
    this.ifNotExists = ifNotExists;
    return this;
  }

  public CreateIndexAttemptBuilder textIndexOptions(
      Boolean caseSensitive, Boolean normalize, Boolean ascii) {
    this.textIndexOptions =
        new CreateIndexAttempt.TextIndexOptions(caseSensitive, normalize, ascii);
    this.textIndexOptions = textIndexOptions;
    return this;
  }

  public CreateIndexAttemptBuilder vectorIndexOptions(
      SimilarityFunction similarityFunction, String sourceModel) {
    this.vectorIndexOptions =
        new CreateIndexAttempt.VectorIndexOptions(similarityFunction, sourceModel);
    return this;
  }

  public CreateIndexAttempt build() {
    // Validate required fields
    if (schemaObject == null || columnName == null || dataType == null || indexName == null) {
      throw new IllegalStateException(
          "SchemaObject, columnName, dataType, and indexName must not be null");
    }

    // Create and return the CreateIndexAttempt object
    return new CreateIndexAttempt(
        position,
        schemaObject,
        columnName,
        dataType,
        indexName,
        textIndexOptions,
        vectorIndexOptions,
        ifNotExists);
  }
}
