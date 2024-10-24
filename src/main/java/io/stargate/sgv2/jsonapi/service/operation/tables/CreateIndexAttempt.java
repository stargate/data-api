package io.stargate.sgv2.jsonapi.service.operation.tables;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateIndex;
import com.datastax.oss.driver.api.querybuilder.schema.CreateIndexOnTable;
import com.datastax.oss.driver.api.querybuilder.schema.CreateIndexStart;
import com.datastax.oss.driver.internal.querybuilder.schema.DefaultCreateIndex;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.override.ExtendedCreateIndex;
import io.stargate.sgv2.jsonapi.service.operation.SchemaAttempt;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import java.util.HashMap;
import java.util.Map;

/*
 An attempt to create index for a table's column.
*/
public class CreateIndexAttempt extends SchemaAttempt<TableSchemaObject> {
  private final CqlIdentifier columnName;
  private final CqlIdentifier indexName;
  private final TextIndexOptions textIndexOptions;
  private final VectorIndexOptions vectorIndexOptions;
  private final DataType dataType;
  private final boolean ifNotExists;

  /*
   * @param position The position of the attempt in the sequence, for create index it's  always 0.
   * @param schemaObject The schema object representing the table.
   * @param columnName The name of the column to create the index on.
   * @param dataType The data type of the column.
   * @param indexName The name of the index to create.
   * @param textIndexOptions The options for a text column type index.
   * @param vectorIndexOptions The options for a vector column type index.
   * @param ifNotExists Flag to ignore if index already exists.
   * @return The attempt to create the index.
   */
  protected CreateIndexAttempt(
      int position,
      TableSchemaObject schemaObject,
      CqlIdentifier columnName,
      DataType dataType,
      CqlIdentifier indexName,
      TextIndexOptions textIndexOptions,
      VectorIndexOptions vectorIndexOptions,
      boolean ifNotExists,
      SchemaAttempt.SchemaRetryPolicy schemaRetryPolicy) {
    super(position, schemaObject, schemaRetryPolicy);

    this.columnName = columnName;
    this.dataType = dataType;
    this.indexName = indexName;
    this.textIndexOptions = textIndexOptions;
    this.vectorIndexOptions = vectorIndexOptions;
    this.ifNotExists = ifNotExists;
    setStatus(OperationStatus.READY);
  }

  /*
   * Options for a text index.
   */
  public record TextIndexOptions(Boolean caseSensitive, Boolean normalize, Boolean ascii) {

    public Map<String, Object> getOptions() {
      Map<String, Object> options = new HashMap<>();
      if (caseSensitive != null) {
        options.put("case_sensitive", caseSensitive);
      }
      if (normalize != null) {
        options.put("normalize", normalize);
      }
      if (ascii != null) {
        options.put("ascii", ascii);
      }
      return options;
    }
  }

  /*
   * Options for a vector index.
   */
  public record VectorIndexOptions(SimilarityFunction similarityFunction, String sourceModel) {
    public Map<String, Object> getOptions() {
      Map<String, Object> options = new HashMap<>();
      if (similarityFunction != null) {
        options.put("similarity_function", similarityFunction.getMetric());
      }
      if (sourceModel != null) {
        options.put("source_model", sourceModel);
      }
      return options;
    }
  }

  @Override
  protected SimpleStatement buildStatement() {
    CqlIdentifier keyspaceIdentifier = cqlIdentifierFromUserInput(schemaObject.name().keyspace());
    CqlIdentifier tableIdentifier = cqlIdentifierFromUserInput(schemaObject.name().table());

    // Set as StorageAttachedIndex as default
    CreateIndexStart createIndexStart =
        SchemaBuilder.createIndex(indexName).custom("StorageAttachedIndex");

    // If `ifNotExists` is true, then set the flag to ignore if index already exists
    if (ifNotExists) {
      createIndexStart = createIndexStart.ifNotExists();
    }
    // Set the keyspace and table name
    final CreateIndexOnTable createIndexOnTable =
        createIndexStart.onTable(keyspaceIdentifier, tableIdentifier);

    // Set the column name
    CreateIndex createIndex;
    if (dataType instanceof MapType) {
      createIndex = createIndexOnTable.andColumnEntries(columnName);
    } else if (dataType instanceof ListType || dataType instanceof SetType) {
      createIndex = createIndexOnTable.andColumnValues(columnName);
    } else {
      createIndex = createIndexOnTable.andColumn(columnName);
    }
    // Set the options for the index
    Map<String, Object> options = new HashMap<>();
    if (textIndexOptions != null && !textIndexOptions.getOptions().isEmpty()) {
      createIndex = createIndex.withOption("OPTIONS", textIndexOptions.getOptions());
    }
    if (vectorIndexOptions != null && !vectorIndexOptions.getOptions().isEmpty()) {
      createIndex = createIndex.withOption("OPTIONS", vectorIndexOptions.getOptions());
    }

    // Hack code to fix the issue with respect to quoted columns
    ExtendedCreateIndex extendedCreateIndex =
        new ExtendedCreateIndex((DefaultCreateIndex) createIndex);

    return extendedCreateIndex.build();
  }
}
