package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.ByteUtils;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.VectorType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class TableSchemaObject extends TableBasedSchemaObject {

  public static final SchemaObjectType TYPE = SchemaObjectType.TABLE;

  private final List<VectorConfig> vectorConfigs;

  private TableSchemaObject(TableMetadata tableMetadata) {
    super(TYPE, tableMetadata);
    vectorConfigs = new ArrayList<>();
  }

  @Override
  public List<VectorConfig> vectorConfigs() {
    return vectorConfigs;
  }

  @Override
  public IndexUsage newIndexUsage() {
    return IndexUsage.NO_OP;
  }

  public static TableSchemaObject getTableSettings(
      TableMetadata tableMetadata, ObjectMapper objectMapper) {
    Map<String, String> extensions =
        (Map<String, String>)
            tableMetadata.getOptions().get(CqlIdentifier.fromInternal("extensions"));
    String vectorize = extensions != null ? extensions.get("vectorize") : null;
    Map<String, VectorConfig.VectorizeConfig> resultMap = new HashMap<>();
    if (vectorize != null) {
      String vectorizeJson = new String(ByteUtils.fromHexString(vectorize).array());
      // Define the TypeReference for Map<String, VectorConfig.VectorizeConfig>
      TypeReference<Map<String, VectorConfig.VectorizeConfig>> typeRef =
          new TypeReference<Map<String, VectorConfig.VectorizeConfig>>() {};

      // Convert JSON string to Map
      try {
        resultMap = objectMapper.readValue(vectorizeJson, typeRef);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }
    TableSchemaObject tableSchemaObject = new TableSchemaObject(tableMetadata);

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
        VectorConfig vectorConfig =
            new VectorConfig(
                true,
                column.getKey().asInternal(),
                dimension,
                similarityFunction,
                resultMap.get(column.getKey().asInternal()));
        tableSchemaObject.vectorConfigs.add(vectorConfig);
      }
    }
    if (tableSchemaObject.vectorConfigs.isEmpty()) {
      tableSchemaObject.vectorConfigs().add(VectorConfig.notEnabledVectorConfig());
    }
    return tableSchemaObject;
  }
}
