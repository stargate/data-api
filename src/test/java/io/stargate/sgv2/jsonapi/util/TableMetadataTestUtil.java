package io.stargate.sgv2.jsonapi.util;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultColumnMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultTableMetadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.stream.Stream;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;

public class TableMetadataTestUtil {

    private TableMetadataTestUtil(){}

    public record TableAndColumn(TableMetadata tableMetadata, CqlIdentifier column){}

    public static Stream<TableAndColumn> removeAllColumns(TableMetadata tableMetadata) {
        return removeAllColumns(tableMetadata, tableMetadata.getColumns().keySet());
    }

    public static Stream<TableAndColumn> removeAllColumns(TableMetadata tableMetadata, Collection<CqlIdentifier> columns) {
        return columns.stream()
                .map(column -> new TableAndColumn(removeColumn(tableMetadata, column), column));
    }

    public static TableMetadata removeColumn(TableMetadata tableMetadata, ColumnMetadata columnMetadata){
        return removeColumn(tableMetadata, columnMetadata.getName());
    }

    public static TableMetadata removeColumn(TableMetadata tableMetadata, CqlIdentifier identifier){

        var columns = new LinkedHashMap<>(tableMetadata.getColumns());
        if ( columns.remove(identifier) == null){
            throw new IllegalStateException("Column not found. identifier:%s, tableMetadata:%s, ".formatted(identifier, tableMetadata.describe(true)));
        }
        return new DefaultTableMetadata(
                tableMetadata.getKeyspace(),
                tableMetadata.getName(),
                tableMetadata.getId().orElseThrow(),
                tableMetadata.isCompactStorage(),
                tableMetadata.isVirtual(),
                tableMetadata.getPartitionKey(),
                tableMetadata.getClusteringColumns(),
                columns,
                tableMetadata.getOptions(),
                tableMetadata.getIndexes()
        );
    }


    public static Stream<TableAndColumn> removeAllPartitionKeys(TableMetadata tableMetadata) {
        return removeAllPartitionKeys(tableMetadata, tableMetadata.getPartitionKey());
    }

    public static Stream<TableAndColumn> removeAllPartitionKeys(TableMetadata tableMetadata, Collection<ColumnMetadata> columns) {
        return columns.stream()
                .map(column -> new TableAndColumn(removePartitionKey(tableMetadata, column), column.getName()));
    }

    public static TableMetadata removePartitionKey(TableMetadata tableMetadata, ColumnMetadata columnMetadata){
        var partitionKeys = new ArrayList<>(tableMetadata.getPartitionKey());
        if (!partitionKeys.remove(columnMetadata)){
            throw new IllegalStateException("PartitionKey not found. columnMetadata:%s, tableMetadata:%s, ".formatted(columnMetadata, tableMetadata.describe(true)));
        }
        return new DefaultTableMetadata(
                tableMetadata.getKeyspace(),
                tableMetadata.getName(),
                tableMetadata.getId().orElseThrow(),
                tableMetadata.isCompactStorage(),
                tableMetadata.isVirtual(),
                partitionKeys,
                tableMetadata.getClusteringColumns(),
                tableMetadata.getColumns(),
                tableMetadata.getOptions(),
                tableMetadata.getIndexes()
        );
    }


    public static Stream<TableAndColumn> swapTypesAllColumns(TableMetadata tableMetadata, DataType swapToType, DataType collisionToType) {
        return swapTypesAllColumns(tableMetadata, tableMetadata.getColumns().keySet(), swapToType, collisionToType);
    }

    public static Stream<TableAndColumn> swapTypesAllColumns(TableMetadata tableMetadata, Collection<CqlIdentifier> columns, DataType swapToType, DataType collisionToType) {
        return columns.stream()
                .map(column -> new TableAndColumn(swapType(tableMetadata, column, swapToType, collisionToType), column));
    }


    public static TableMetadata swapType(TableMetadata tableMetadata, CqlIdentifier identifier, DataType swapToType, DataType collisionToType){

        var localColumns = new LinkedHashMap<>(tableMetadata.getColumns());
        var existingColumn = localColumns.get(identifier);
        if (existingColumn == null){
            throw new IllegalStateException("Column not found. identifier:%s, tableMetadata:%s, ".formatted(identifier, tableMetadata.describe(true)));
        }
        var newType = existingColumn.getType() == swapToType ? collisionToType : swapToType;
        var newColumn = new DefaultColumnMetadata(
                existingColumn.getKeyspace(),
                existingColumn.getParent(),
                existingColumn.getName(),
                newType,
                existingColumn.isStatic());
        localColumns.put(identifier, newColumn);

        return new DefaultTableMetadata(
                tableMetadata.getKeyspace(),
                tableMetadata.getName(),
                tableMetadata.getId().orElseThrow(),
                tableMetadata.isCompactStorage(),
                tableMetadata.isVirtual(),
                tableMetadata.getPartitionKey(),
                tableMetadata.getClusteringColumns(),
                localColumns,
                tableMetadata.getOptions(),
                tableMetadata.getIndexes()
        );
    }

    public static TableMetadata addPartitionKey(TableMetadata tableMetadata, boolean clearFirst, String name, DataType datatype) {

        var column = new DefaultColumnMetadata(
                tableMetadata.getKeyspace(),
                tableMetadata.getName(),
                cqlIdentifierFromUserInput(name),
                datatype,
                false
        );
        return addPartitionKey(tableMetadata, clearFirst, column);
    }

    public static TableMetadata addPartitionKey(TableMetadata tableMetadata,boolean clearFirst, ColumnMetadata columnMetadata){

        var partitionKeys = new ArrayList<>(tableMetadata.getPartitionKey());
        if (clearFirst){
            partitionKeys.clear();
        }
        partitionKeys.add(columnMetadata);

        return new DefaultTableMetadata(
                tableMetadata.getKeyspace(),
                tableMetadata.getName(),
                tableMetadata.getId().orElseThrow(),
                tableMetadata.isCompactStorage(),
                tableMetadata.isVirtual(),
                partitionKeys,
                tableMetadata.getClusteringColumns(),
                tableMetadata.getColumns(),
                tableMetadata.getOptions(),
                tableMetadata.getIndexes()
        );
    }

    public static TableMetadata addClusteringColumn(TableMetadata tableMetadata, String name, DataType datatype) {

        var column = new DefaultColumnMetadata(
                tableMetadata.getKeyspace(),
                tableMetadata.getName(),
                cqlIdentifierFromUserInput(name),
                datatype,
                false
        );
        return addClusteringColumn(tableMetadata, column, ClusteringOrder.ASC);
    }

    public static TableMetadata addClusteringColumn(TableMetadata tableMetadata, ColumnMetadata columnMetadata, ClusteringOrder clusteringOrder){

        var clusteringColumns = new LinkedHashMap<>(tableMetadata.getClusteringColumns());
        clusteringColumns.put(columnMetadata, clusteringOrder);

        return new DefaultTableMetadata(
                tableMetadata.getKeyspace(),
                tableMetadata.getName(),
                tableMetadata.getId().orElseThrow(),
                tableMetadata.isCompactStorage(),
                tableMetadata.isVirtual(),
                tableMetadata.getPartitionKey(),
                clusteringColumns,
                tableMetadata.getColumns(),
                tableMetadata.getOptions(),
                tableMetadata.getIndexes()
        );
    }

    public static TableMetadata addColumn(TableMetadata tableMetadata, String name, DataType datatype) {

        var column = new DefaultColumnMetadata(
                tableMetadata.getKeyspace(),
                tableMetadata.getName(),
                cqlIdentifierFromUserInput(name),
                datatype,
                false
        );
        return addColumn(tableMetadata, column);
    }

    public static TableMetadata addColumn(TableMetadata tableMetadata, ColumnMetadata columnMetadata){

        var columns = new LinkedHashMap<>(tableMetadata.getColumns());
        columns.put(columnMetadata.getName(), columnMetadata);

        return new DefaultTableMetadata(
                tableMetadata.getKeyspace(),
                tableMetadata.getName(),
                tableMetadata.getId().orElseThrow(),
                tableMetadata.isCompactStorage(),
                tableMetadata.isVirtual(),
                tableMetadata.getPartitionKey(),
                tableMetadata.getClusteringColumns(),
                columns,
                tableMetadata.getOptions(),
                tableMetadata.getIndexes()
        );
    }
}
