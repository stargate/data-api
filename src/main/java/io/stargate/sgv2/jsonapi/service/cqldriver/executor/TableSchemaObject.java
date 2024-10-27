package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.config.constants.TableIndexConstants;
import io.stargate.sgv2.jsonapi.config.constants.VectorConstant;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import io.stargate.sgv2.jsonapi.service.schema.tables.IndexDefinition;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TableSchemaObject extends TableBasedSchemaObject {

  public static final SchemaObjectType TYPE = SchemaObjectType.TABLE;

  private final VectorConfig vectorConfig;

  private final IndexConfig indexConfigs;

  private TableSchemaObject(
      TableMetadata tableMetadata, VectorConfig vectorConfig, IndexConfig indexConfig) {
    super(TYPE, tableMetadata);
    this.vectorConfig = vectorConfig;
    this.indexConfigs = indexConfig;
  }

  @Override
  public VectorConfig vectorConfig() {
    return vectorConfig;
  }

  @Override
  public IndexUsage newIndexUsage() {
    return IndexUsage.NO_OP;
  }

  public IndexConfig indexConfig() {
    return indexConfigs;
  }

  /** Get table schema object from table metadata */
  public static TableSchemaObject from(TableMetadata tableMetadata, ObjectMapper objectMapper) {
    Map<String, String> extensions = TableMetadataUtils.getExtensions(tableMetadata);
    Map<String, VectorConfig.ColumnVectorDefinition.VectorizeConfig> vectorizeConfigMap =
        TableMetadataUtils.getVectorizeMap(extensions, objectMapper);
    IndexConfig indexConfig = IndexConfig.from(tableMetadata);
    VectorConfig vectorConfig;
    List<VectorConfig.ColumnVectorDefinition> columnVectorDefinitions = new ArrayList<>();
    for (Map.Entry<CqlIdentifier, ColumnMetadata> column : tableMetadata.getColumns().entrySet()) {
      var indexDefinition =
          indexConfig.get(CqlIdentifierUtil.cqlIdentifierToStringForUser(column.getKey()));
      if (indexDefinition.indexType() == IndexDefinition.IndexType.VECTOR) {
        SimilarityFunction similarityFunction = SimilarityFunction.COSINE;
        if (indexDefinition != null) {
          var sourceModel =
              indexDefinition
                  .options()
                  .get(TableIndexConstants.IndexOptionKeys.SOURCE_MODEL_OPTION);
          var similarityFunctionValue =
              indexDefinition
                  .options()
                  .get(TableIndexConstants.IndexOptionKeys.SIMILARITY_FUNCTION_OPTION);
          if (similarityFunctionValue != null) {
            similarityFunction = SimilarityFunction.fromString(similarityFunctionValue);
          } else if (sourceModel != null) {
            similarityFunction = VectorConstant.SUPPORTED_SOURCES.get(sourceModel);
          }
        }
        int dimension = vectorType.getDimensions();
        VectorConfig.ColumnVectorDefinition columnVectorDefinition =
            new VectorConfig.ColumnVectorDefinition(
                column.getKey().asInternal(),
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
    return new TableSchemaObject(tableMetadata, vectorConfig, indexConfig);
  }
}
