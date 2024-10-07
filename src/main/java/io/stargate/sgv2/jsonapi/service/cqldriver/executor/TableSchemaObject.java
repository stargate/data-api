package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.ByteUtils;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.VectorType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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

  /**
   * Get table schema object from table metadata
   *
   * @param tableMetadata
   * @param objectMapper
   * @return
   */
  public static TableSchemaObject getTableSettings(
      TableMetadata tableMetadata, ObjectMapper objectMapper) {
    Map<String, String> extensions =
        (Map<String, String>)
            tableMetadata.getOptions().get(CqlIdentifier.fromInternal("extensions"));
    String vectorize = extensions != null ? extensions.get("vectorize") : null;
    Map<String, VectorConfig.ColumnVectorDefinition.VectorizeConfig> resultMap = new HashMap<>();
    if (vectorize != null) {
      try {
        String vectorizeJson =
            new String(ByteUtils.fromHexString(vectorize).array(), StandardCharsets.UTF_8);
        // Convert JSON string to Map
        JsonNode vectorizeByColumns = objectMapper.readTree(vectorizeJson);
        Map<String, VectorConfig.ColumnVectorDefinition.VectorizeConfig> vectorizeConfigMap =
            new HashMap<>();
        while (vectorizeByColumns.fields().hasNext()) {
          Map.Entry<String, JsonNode> entry = vectorizeByColumns.fields().next();
          VectorConfig.ColumnVectorDefinition.VectorizeConfig vectorizeConfig =
              objectMapper.treeToValue(
                  entry.getValue(), VectorConfig.ColumnVectorDefinition.VectorizeConfig.class);
          vectorizeConfigMap.put(entry.getKey(), vectorizeConfig);
        }
      } catch (JsonProcessingException | IllegalArgumentException e) {
        throw SchemaException.Code.INVALID_VECTORIZE_CONFIGURATION.get();
      }
    }
    VectorConfig vectorConfig;
    List<VectorConfig.ColumnVectorDefinition> columnVectorDefinitions = new ArrayList<>();
    for (Map.Entry<CqlIdentifier, ColumnMetadata> column : tableMetadata.getColumns().entrySet()) {
      if (column.getValue().getType() instanceof VectorType vectorType) {
        final Optional<IndexMetadata> index = tableMetadata.getIndex(column.getKey());
        SimilarityFunction similarityFunction = SimilarityFunction.COSINE;
        if (index.isPresent()) {
          final IndexMetadata indexMetadata = index.get();
          final Map<String, String> indexOptions = indexMetadata.getOptions();
          final String similarityFunctionValue = indexOptions.get("similarity_function");
          if (similarityFunctionValue != null) {
            similarityFunction = SimilarityFunction.fromString(similarityFunctionValue);
          }
        }
        int dimension = vectorType.getDimensions();
        VectorConfig.ColumnVectorDefinition columnVectorDefinition =
            new VectorConfig.ColumnVectorDefinition(
                column.getKey().asInternal(),
                dimension,
                similarityFunction,
                resultMap.get(column.getKey().asInternal()));
        columnVectorDefinitions.add(columnVectorDefinition);
      }
    }
    if (columnVectorDefinitions.isEmpty()) {
      vectorConfig = VectorConfig.notEnabledVectorConfig();
    } else {
      vectorConfig = new VectorConfig(true, Collections.unmodifiableList(columnVectorDefinitions));
    }
    return new TableSchemaObject(tableMetadata, vectorConfig);
  }
}
