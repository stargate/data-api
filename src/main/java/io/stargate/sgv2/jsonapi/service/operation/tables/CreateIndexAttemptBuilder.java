package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.SchemaAttempt;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import java.util.Objects;

/** Builder for a {@link CreateIndexAttempt}. */
public class CreateIndexAttemptBuilder {
  private int position;
  private TableSchemaObject schemaObject;
  private CqlIdentifier columnName;
  private DataType dataType;
  private CqlIdentifier indexName;
  private CreateIndexAttempt.TextIndexOptions textIndexOptions;
  private CreateIndexAttempt.VectorIndexOptions vectorIndexOptions;
  private SchemaAttempt.SchemaRetryPolicy schemaRetryPolicy;
  private boolean ifNotExists;

  public CreateIndexAttemptBuilder(
      int position,
      TableSchemaObject schemaObject,
      String columnName,
      String indexName,
      SchemaAttempt.SchemaRetryPolicy schemaRetryPolicy) {
    this.position = position;
    this.schemaObject = schemaObject;
    this.columnName = CqlIdentifierUtil.cqlIdentifierFromUserInput(columnName);
    this.indexName = CqlIdentifierUtil.cqlIdentifierFromUserInput(indexName);
    this.dataType = schemaObject.tableMetadata().getColumn(this.columnName).get().getType();
    this.schemaRetryPolicy = schemaRetryPolicy;
  }

  public CreateIndexAttemptBuilder ifNotExists(boolean ifNotExists) {
    this.ifNotExists = ifNotExists;
    return this;
  }

  public CreateIndexAttemptBuilder textIndexOptions(
      Boolean caseSensitive, Boolean normalize, Boolean ascii) {
    this.textIndexOptions =
        new CreateIndexAttempt.TextIndexOptions(caseSensitive, normalize, ascii);
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
    Objects.requireNonNull(columnName, "Column name cannot be null");
    Objects.requireNonNull(dataType, "Data type cannot be null");
    Objects.requireNonNull(indexName, "Index name cannot be null");

    // Create and return the CreateIndexAttempt object
    return new CreateIndexAttempt(
        position++,
        schemaObject,
        columnName,
        dataType,
        indexName,
        textIndexOptions,
        vectorIndexOptions,
        ifNotExists,
        schemaRetryPolicy);
  }
}
