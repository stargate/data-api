package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.SchemaAttempt;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiRegularIndex;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import java.util.Objects;

/** Builder for a {@link CreateIndexAttempt}. */
public class CreateIndexAttemptBuilder {
  private int position;

  private final TableSchemaObject schemaObject;
  private SchemaAttempt.SchemaRetryPolicy schemaRetryPolicy;
  // must be specified, the default should not be defined in here
  private Boolean ifNotExists = null;

  public CreateIndexAttemptBuilder(TableSchemaObject schemaObject) {
    this.schemaObject =  Objects.requireNonNull(schemaObject, "schemaObject object cannot be null");
  }

  public CreateIndexAttemptBuilder withIfNotExists(boolean ifNotExists) {
    this.ifNotExists = ifNotExists;
    return this;
  }

  public CreateIndexAttemptBuilder withSchemaRetryPolicy(SchemaAttempt.SchemaRetryPolicy schemaRetryPolicy) {
    this.schemaRetryPolicy = schemaRetryPolicy;
    return this;
  }



//  public CreateIndexAttemptBuilder vectorIndexOptions(
//      SimilarityFunction similarityFunction, String sourceModel) {
//    this.vectorIndexOptions =
//        new CreateIndexAttempt.VectorIndexOptions(similarityFunction, sourceModel);
//    return this;
//  }

  public CreateIndexAttempt build(ApiRegularIndex indexDef) {
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
