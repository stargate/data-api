package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.ByteUtils;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.VectorType;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.impl.VectorizeConfig;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.PrimaryKey;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnType;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ComplexTypes;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.PrimitiveTypes;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataTypeDef;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataTypeDefs;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class TableSchemaObject extends TableBasedSchemaObject {

  public static final SchemaObjectType TYPE = SchemaObjectType.TABLE;

  private final List<VectorConfig> vectorConfigs;

  private TableSchemaObject(TableMetadata tableMetadata, List<VectorConfig> vectorConfigs) {
    super(TYPE, tableMetadata);
    this.vectorConfigs = vectorConfigs;
  }

  @Override
  public List<VectorConfig> vectorConfigs() {
    return vectorConfigs;
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

    List<VectorConfig> vectorConfigs = new ArrayList<>();
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
        vectorConfigs.add(vectorConfig);
      }
    }
    if (vectorConfigs.isEmpty()) {
      vectorConfigs.add(VectorConfig.notEnabledVectorConfig());
    }
    return new TableSchemaObject(tableMetadata, Collections.unmodifiableList(vectorConfigs));
  }

  public TableResponse toTableResponse() {
    String tableName = name().table();
    HashMap<String, ColumnType> columnsDefinition = new HashMap<>();
    for (Map.Entry<CqlIdentifier, ColumnMetadata> column :
        tableMetadata().getColumns().entrySet()) {
      ColumnType type = getColumnType(column.getKey().asInternal(), column.getValue());
      columnsDefinition.put(column.getKey().asInternal(), type);
    }

    final List<String> partitionBy =
        tableMetadata().getPartitionKey().stream()
            .map(column -> column.getName().asInternal())
            .collect(Collectors.toList());
    final List<PrimaryKey.OrderingKey> partitionSort =
        tableMetadata().getClusteringColumns().entrySet().stream()
            .map(
                entry ->
                    new PrimaryKey.OrderingKey(
                        entry.getKey().getName().asInternal(),
                        entry.getValue() == ClusteringOrder.ASC
                            ? PrimaryKey.OrderingKey.Order.ASC
                            : PrimaryKey.OrderingKey.Order.DESC))
            .collect(Collectors.toList());
    PrimaryKey primaryKey =
        new PrimaryKey(
            partitionBy.toArray(new String[0]),
            partitionSort.toArray(new PrimaryKey.OrderingKey[0]));
    return new TableResponse(
        tableName,
        new TableResponse.TableDefinition(
            new TableResponse.TableDefinition.ColumnsDefinition(columnsDefinition), primaryKey));
  }

  // Need to handle frozen types
  private ColumnType getColumnType(String columnName, ColumnMetadata columnMetadata) {
    if (columnMetadata.getType() instanceof VectorType vt) {
      // Schema will always have VectorConfig for vector type
      VectorConfig vectorConfig =
          vectorConfigs.stream().filter(vc -> vc.fieldName().equals(columnName)).findFirst().get();
      VectorizeConfig vectorizeConfig =
          vectorConfig.vectorizeConfig() == null
              ? null
              : new VectorizeConfig(
                  vectorConfig.vectorizeConfig().provider(),
                  vectorConfig.vectorizeConfig().modelName(),
                  vectorConfig.vectorizeConfig().authentication(),
                  vectorConfig.vectorizeConfig().parameters());
      return new ComplexTypes.VectorType(PrimitiveTypes.FLOAT, vt.getDimensions(), vectorizeConfig);
    } else if (columnMetadata.getType() instanceof MapType mt) {
      return new ComplexTypes.MapType(
          PrimitiveTypes.fromString(mt.getKeyType().toString()),
          PrimitiveTypes.fromString(mt.getValueType().toString()));
    } else if (columnMetadata.getType()
        instanceof com.datastax.oss.driver.api.core.type.ListType lt) {
      return new ComplexTypes.ListType(PrimitiveTypes.fromString(lt.getElementType().toString()));
    } else if (columnMetadata.getType()
        instanceof com.datastax.oss.driver.api.core.type.SetType st) {
      return new ComplexTypes.SetType(PrimitiveTypes.fromString(st.getElementType().toString()));
    } else {
      final Optional<ApiDataTypeDef> apiDataTypeDef =
          ApiDataTypeDefs.from(columnMetadata.getType());
      if (apiDataTypeDef.isPresent())
        return PrimitiveTypes.fromString(apiDataTypeDef.get().getApiType().getApiName());
      else {
        // Need to return unsupported type
        throw new RuntimeException("Unknown data type: " + columnMetadata.getType());
      }
    }
  }

  @JsonPropertyOrder({"_id", "definition"})
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record TableResponse(String name, TableDefinition tableDefinition) {

    @JsonPropertyOrder({"columns", "primaryKey"})
    @JsonInclude(JsonInclude.Include.NON_NULL)
    record TableDefinition(ColumnsDefinition columns, PrimaryKey primaryKey) {

      record ColumnsDefinition(Map<String, ColumnType> columns) {}
    }
  }
}
