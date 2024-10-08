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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.impl.VectorizeConfig;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.PrimaryKey;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnType;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ComplexTypes;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.PrimitiveTypes;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataTypeDef;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataTypeDefs;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

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
            VectorConfig.ColumnVectorDefinition.VectorizeConfig vectorizeConfig =
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
                vectorizeConfigMap.get(column.getKey().asInternal()));
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

  /**
   * Convert table schema object to table response which is returned as response for `listTables`
   *
   * @return
   */
  public TableResponse toTableResponse() {
    String tableName = CqlIdentifierUtil.externalRepresentation(tableMetadata().getName());
    HashMap<String, ColumnType> columnsDefinition = new HashMap<>();
    for (Map.Entry<CqlIdentifier, ColumnMetadata> column :
        tableMetadata().getColumns().entrySet()) {
      ColumnType type = getColumnType(column.getKey().asInternal(), column.getValue());
      columnsDefinition.put(CqlIdentifierUtil.externalRepresentation(column.getKey()), type);
    }

    final List<String> partitionBy =
        tableMetadata().getPartitionKey().stream()
            .map(column -> CqlIdentifierUtil.externalRepresentation(column.getName()))
            .collect(Collectors.toList());
    final List<PrimaryKey.OrderingKey> partitionSort =
        tableMetadata().getClusteringColumns().entrySet().stream()
            .map(
                entry ->
                    new PrimaryKey.OrderingKey(
                        CqlIdentifierUtil.externalRepresentation(entry.getKey().getName()),
                        entry.getValue() == ClusteringOrder.ASC
                            ? PrimaryKey.OrderingKey.Order.ASC
                            : PrimaryKey.OrderingKey.Order.DESC))
            .collect(Collectors.toList());
    PrimaryKey primaryKey =
        new PrimaryKey(
            partitionBy.toArray(new String[0]),
            partitionSort.toArray(new PrimaryKey.OrderingKey[0]));
    return new TableResponse(
        tableName, new TableResponse.TableDefinition(columnsDefinition, primaryKey));
  }

  private ColumnType getColumnType(String columnName, ColumnMetadata columnMetadata) {
    if (columnMetadata.getType() instanceof VectorType vt) {
      // Schema will always have VectorConfig for vector type
      VectorConfig.ColumnVectorDefinition columnVectorDefinition =
          vectorConfig.columnVectorDefinitions().stream()
              .filter(vc -> vc.fieldName().equals(columnName))
              .findFirst()
              .get();
      VectorizeConfig vectorizeConfig =
          columnVectorDefinition.vectorizeConfig() == null
              ? null
              : new VectorizeConfig(
                  columnVectorDefinition.vectorizeConfig().provider(),
                  columnVectorDefinition.vectorizeConfig().modelName(),
                  columnVectorDefinition.vectorizeConfig().authentication(),
                  columnVectorDefinition.vectorizeConfig().parameters());
      return new ComplexTypes.VectorType(PrimitiveTypes.FLOAT, vt.getDimensions(), vectorizeConfig);
    } else if (columnMetadata.getType() instanceof MapType mt) {
      if (!mt.isFrozen()) {
        final Optional<ApiDataTypeDef> apiDataTypeDefKey = ApiDataTypeDefs.from(mt.getKeyType());
        final Optional<ApiDataTypeDef> apiDataTypeDefValue =
            ApiDataTypeDefs.from(mt.getValueType());
        if (apiDataTypeDefKey.isPresent() && apiDataTypeDefValue.isPresent()) {
          return new ComplexTypes.MapType(
              PrimitiveTypes.fromString(apiDataTypeDefKey.get().getApiType().getApiName()),
              PrimitiveTypes.fromString(apiDataTypeDefValue.get().getApiType().getApiName()));
        }
      }
      // return unsupported format
      return new ComplexTypes.UnsupportedType(mt.asCql(true, false));

    } else if (columnMetadata.getType()
        instanceof com.datastax.oss.driver.api.core.type.ListType lt) {
      if (!lt.isFrozen()) {
        final Optional<ApiDataTypeDef> apiDataTypeDef = ApiDataTypeDefs.from(lt.getElementType());
        if (apiDataTypeDef.isPresent()) {
          return new ComplexTypes.ListType(
              PrimitiveTypes.fromString(apiDataTypeDef.get().getApiType().getApiName()));
        }
      }
      // return unsupported format
      return new ComplexTypes.UnsupportedType(lt.asCql(true, false));

    } else if (columnMetadata.getType()
        instanceof com.datastax.oss.driver.api.core.type.SetType st) {
      if (!st.isFrozen()) {
        final Optional<ApiDataTypeDef> apiDataTypeDef = ApiDataTypeDefs.from(st.getElementType());
        if (apiDataTypeDef.isPresent()) {
          return new ComplexTypes.SetType(
              PrimitiveTypes.fromString(apiDataTypeDef.get().getApiType().getApiName()));
        }
      }
      // return unsupported format
      return new ComplexTypes.UnsupportedType(st.asCql(true, false));
    } else {
      final Optional<ApiDataTypeDef> apiDataTypeDef =
          ApiDataTypeDefs.from(columnMetadata.getType());
      if (apiDataTypeDef.isPresent())
        return PrimitiveTypes.fromString(apiDataTypeDef.get().getApiType().getApiName());
      else {
        // Need to return unsupported type
        return new ComplexTypes.UnsupportedType(columnMetadata.getType().asCql(true, false));
      }
    }
  }

  /**
   * Object used to build the response for listTables command
   *
   * @param name
   * @param definition
   */
  @JsonPropertyOrder({"name", "definition"})
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record TableResponse(String name, TableDefinition definition) {

    @JsonPropertyOrder({"columns", "primaryKey"})
    @JsonInclude(JsonInclude.Include.NON_NULL)
    record TableDefinition(Map<String, ColumnType> columns, PrimaryKey primaryKey) {}
  }
}
