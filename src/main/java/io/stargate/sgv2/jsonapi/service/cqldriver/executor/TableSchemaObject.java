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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
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
  public static TableSchemaObject from(TableMetadata tableMetadata, ObjectMapper objectMapper) {
    Map<String, ByteBuffer> extensions =
        (Map<String, ByteBuffer>)
            tableMetadata.getOptions().get(CqlIdentifier.fromInternal("extensions"));
    String vectorizeJson = null;
    if (extensions != null) {
      ByteBuffer vectorizeBuffer =
          (ByteBuffer) extensions.get("com.datastax.data-api.vectorize-config");
      vectorizeJson =
          vectorizeBuffer != null
              ? new String(ByteUtils.getArray(vectorizeBuffer.duplicate()), StandardCharsets.UTF_8)
              : null;
    }
    Map<String, VectorConfig.ColumnVectorDefinition.VectorizeConfig> vectorizeConfigMap =
        new HashMap<>();
    if (vectorizeJson != null) {
      try {
        JsonNode vectorizeByColumns = objectMapper.readTree(vectorizeJson);
        Iterator<Map.Entry<String, JsonNode>> it = vectorizeByColumns.fields();
        while (it.hasNext()) {
          Map.Entry<String, JsonNode> entry = it.next();
          try {
            var vectorizeConfig =
                objectMapper.treeToValue(
                    entry.getValue(), VectorConfig.ColumnVectorDefinition.VectorizeConfig.class);
            vectorizeConfigMap.put(entry.getKey(), vectorizeConfig);
          } catch (JsonProcessingException | IllegalArgumentException e) {
            throw SchemaException.Code.INVALID_VECTORIZE_CONFIGURATION.get(
                Map.of("field", entry.getKey()));
          }
        }
      } catch (JsonProcessingException e) {
        throw SchemaException.Code.INVALID_CONFIGURATION.get();
      }
    }
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
