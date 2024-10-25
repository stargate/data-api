package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import io.stargate.sgv2.jsonapi.api.model.command.table.TableDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.ColumnsDef;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.PrimaryKey;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.TableDefinition;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class ApiTableDef {

  private final CqlIdentifier name;
  private final ApiColumnDefContainer primaryKeys;
  private final ApiColumnDefContainer partitionkeys;
  private final List<ApiClusteringDef> clusteringkeys;
  private final ApiColumnDefContainer allColumns;
  private final ApiColumnDefContainer nonPKColumns;
  private final ApiColumnDefContainer unsupportedColumns;

  private ApiTableDef(
      CqlIdentifier name,
      ApiColumnDefContainer primaryKeys,
      ApiColumnDefContainer partitionkeys,
      List<ApiClusteringDef> clusteringkeys,
      ApiColumnDefContainer allColumns,
      ApiColumnDefContainer nonPKColumns) {
    this.name = name;
    this.primaryKeys = primaryKeys.toUnmodifiable();
    this.partitionkeys = partitionkeys.toUnmodifiable();
    this.clusteringkeys = Collections.unmodifiableList(clusteringkeys);
    this.allColumns = allColumns.toUnmodifiable();
    this.nonPKColumns = nonPKColumns.toUnmodifiable();

    this.unsupportedColumns =
        allColumns.values().stream()
            .filter(columnDef -> columnDef.type().isUnsupported())
            .collect(
                ApiColumnDefContainer::new,
                ApiColumnDefContainer::put,
                ApiColumnDefContainer::putAll);
  }

  public static ApiTableDef from(TableMetadata tableMetadata, VectorConfig vectorConfig) {
    Objects.requireNonNull(tableMetadata, "tableMetadata cannot be null");
    Objects.requireNonNull(vectorConfig, "vectorConfig cannot be null");

    var allColumns = ApiColumnDefContainer.from(tableMetadata.getColumns().values(), vectorConfig);

    var primaryKeys = new ApiColumnDefContainer(tableMetadata.getPrimaryKey().size());
    tableMetadata.getPrimaryKey().stream()
        .map(metadata -> allColumns.get(metadata.getName()))
        .forEach(primaryKeys::put);

    var partitionKeys = new ApiColumnDefContainer(tableMetadata.getPartitionKey().size());
    tableMetadata.getPartitionKey().stream()
        .map(metadata -> allColumns.get(metadata.getName()))
        .forEach(partitionKeys::put);

    var clusteringKeys =
        new ArrayList<ApiClusteringDef>(tableMetadata.getClusteringColumns().size());
    tableMetadata.getClusteringColumns().entrySet().stream()
        .map(
            entry ->
                ApiClusteringDef.from(allColumns.get(entry.getKey().getName()), entry.getValue()))
        .forEach(clusteringKeys::add);

    var nonPKColumns = new ApiColumnDefContainer(allColumns.size() - primaryKeys.size());
    allColumns.values().stream()
        .filter(columnDef -> !primaryKeys.contains(columnDef))
        .forEach(nonPKColumns::put);

    return new ApiTableDef(
        tableMetadata.getName(),
        primaryKeys,
        partitionKeys,
        clusteringKeys,
        allColumns,
        nonPKColumns);
  }

  public TableDesc toTableDesc() {

    var partitionKeys = partitionkeys.values().stream().map(ApiColumnDef::name).toList();
    var orderingKeys =
        clusteringkeys.stream()
            .map(
                clusteringDef ->
                    PrimaryKey.OrderingKey.from(
                        clusteringDef.columnDef().name(), clusteringDef.order()))
            .toList();
    var primaryKey = PrimaryKey.from(partitionKeys, orderingKeys);

    var columnsDesc = new ColumnsDef();
    allColumns
        .values()
        .forEach(columnDef -> columnsDesc.put(columnDef.name(), columnDef.type().getColumnType()));

    return TableDesc.from(name, new TableDefinition(columnsDesc, primaryKey));
  }

  public CqlIdentifier name() {
    return name;
  }

  public ApiColumnDefContainer primaryKeys() {
    return primaryKeys;
  }

  public ApiColumnDefContainer partitionKeys() {
    return partitionkeys;
  }

  public List<ApiClusteringDef> clusteringKeys() {
    return clusteringkeys;
  }

  public ApiColumnDefContainer allColumns() {
    return allColumns;
  }

  public ApiColumnDefContainer nonPKColumns() {
    return nonPKColumns;
  }

  public ApiColumnDefContainer unsupportedColumns() {
    return unsupportedColumns;
  }
}
