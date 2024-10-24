package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToJsonKey;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.VectorType;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.config.constants.VectorConstant;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class TableSchemaObject extends TableBasedSchemaObject {

  public static final SchemaObjectType TYPE = SchemaObjectType.TABLE;

  private final VectorConfig vectorConfig;

  private TableSchemaObject(TableMetadata tableMetadata, VectorConfig vectorConfig) {
    super(TYPE, tableMetadata);
    this.vectorConfig = vectorConfig;
  }

  @Override
  public VectorConfig vectorConfig() {
    return vectorConfig;
  }

  @Override
  public IndexUsage newIndexUsage() {
    return IndexUsage.NO_OP;
  }

  /** Get table schema object from table metadata */
  public static TableSchemaObject from(TableMetadata tableMetadata, ObjectMapper objectMapper) {

    Map<String, String> extensions = TableMetadataUtils.getExtensions(tableMetadata);
    Map<String, VectorConfig.ColumnVectorDefinition.VectorizeDefinition> vectorizeConfigMap =
        TableMetadataUtils.getVectorizeMap(extensions, objectMapper);
    VectorConfig vectorConfig;

    List<VectorConfig.ColumnVectorDefinition> columnVectorDefinitions = new ArrayList<>();
    for (Map.Entry<CqlIdentifier, ColumnMetadata> column : tableMetadata.getColumns().entrySet()) {

      if (column.getValue().getType() instanceof VectorType vectorType) {
        final Optional<IndexMetadata> index =
            tableMetadata.getIndexes().values().stream()
                .filter(
                    indexMetadata -> indexMetadata.getTarget().equals(column.getKey().asCql(true)))
                .findFirst();
        SimilarityFunction similarityFunction = SimilarityFunction.COSINE;

        if (index.isPresent()) {
          final IndexMetadata indexMetadata = index.get();
          final Map<String, String> indexOptions = indexMetadata.getOptions();
          final String sourceModel = indexOptions.get("source_model");
          final String similarityFunctionValue = indexOptions.get("similarity_function");
          if (similarityFunctionValue != null) {
            similarityFunction = SimilarityFunction.fromString(similarityFunctionValue);
          } else if (sourceModel != null) {
            similarityFunction = VectorConstant.SUPPORTED_SOURCES.get(sourceModel);
          }
        }
        int dimension = vectorType.getDimensions();
        // NOTE: need to keep the column name as a string in the ColumnVectorDefinition
        // because also used by collections
        VectorConfig.ColumnVectorDefinition columnVectorDefinition =
            new VectorConfig.ColumnVectorDefinition(
                cqlIdentifierToJsonKey(column.getKey()),
                dimension,
                similarityFunction,
                vectorizeConfigMap.get(column.getKey().asInternal()));
        columnVectorDefinitions.add(columnVectorDefinition);
      }
    }

    if (columnVectorDefinitions.isEmpty()) {
      vectorConfig = VectorConfig.NOT_ENABLED_CONFIG;
    } else {
      vectorConfig =
          VectorConfig.fromColumnDefinitions(Collections.unmodifiableList(columnVectorDefinitions));
    }
    return new TableSchemaObject(tableMetadata, vectorConfig);
  }
}
