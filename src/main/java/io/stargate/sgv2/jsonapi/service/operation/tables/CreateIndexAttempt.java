package io.stargate.sgv2.jsonapi.service.operation.tables;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateIndex;
import com.datastax.oss.driver.api.querybuilder.schema.CreateIndexOnTable;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.SchemaAttempt;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/** An attempt to create index for a table's column */
public class CreateIndexAttempt extends SchemaAttempt<TableSchemaObject> {
  private final String columnName;
  private final String indexName;
  private final TextIndexOptions textIndexOptions;
  private final VectorIndexOptions vectorIndexOptions;
  private final DataType dataType;

  protected CreateIndexAttempt(
      int position,
      TableSchemaObject schemaObject,
      String columnName,
      DataType dataType,
      String indexName,
      TextIndexOptions textIndexOptions,
      VectorIndexOptions vectorIndexOptions) {
    super(position, schemaObject, new SchemaRetryPolicy(2, Duration.ofMillis(10)));

    this.columnName = columnName;
    this.dataType = dataType;
    this.indexName = indexName;
    this.textIndexOptions = textIndexOptions;
    this.vectorIndexOptions = vectorIndexOptions;

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
        options.put("similarity_function", similarityFunction);
      }
      if (sourceModel != null) {
        options.put("source_model", sourceModel);
      }
      return options;
    }
  }

  @Override
  protected SimpleStatement buildStatement() {
    var keyspaceIdentifier = cqlIdentifierFromUserInput(schemaObject.name().keyspace());
    var tableIdentifier = cqlIdentifierFromUserInput(schemaObject.name().table());
    final CreateIndexOnTable indexTableUsingSai =
        SchemaBuilder.createIndex()
            .custom("StorageAttachedIndex")
            .onTable(keyspaceIdentifier, tableIdentifier);
    CreateIndex createIndex;
    if (dataType instanceof MapType) {
      createIndex = indexTableUsingSai.andColumnEntries(cqlIdentifierFromUserInput(columnName));
    } else if (dataType instanceof ListType || dataType instanceof SetType) {
      createIndex = indexTableUsingSai.andColumnValues(cqlIdentifierFromUserInput(columnName));
    } else {
      createIndex = indexTableUsingSai.andColumn(cqlIdentifierFromUserInput(columnName));
    }
    Map<String, Object> options = new HashMap<>();
    if (textIndexOptions != null) {
      for (Map.Entry<String, Object> entry : textIndexOptions.getOptions().entrySet()) {
        createIndex = createIndex.withOption(entry.getKey(), entry.getValue());
      }
    }
    if (vectorIndexOptions != null) {
      for (Map.Entry<String, Object> entry : vectorIndexOptions.getOptions().entrySet()) {
        createIndex = createIndex.withOption(entry.getKey(), entry.getValue());
      }
    }
    return createIndex.build();
  }
}
