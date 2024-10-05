package io.stargate.sgv2.jsonapi.fixtures.testdata;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.NativeTypeTableFilter;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.TextTableFilter;
import io.stargate.sgv2.jsonapi.service.operation.query.DBLogicalExpression;
import io.stargate.sgv2.jsonapi.service.operation.query.TableFilter;
import java.util.List;
import java.util.Set;

public class LogicalExpressionTestData extends TestDataSuplier {

  public LogicalExpressionTestData(TestData testData) {
    super(testData);
  }

  public DBLogicalExpression eqAllPartitionKeys(TableMetadata tableMetadata) {
    var exp = new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
    tableMetadata
        .getPartitionKey()
        .forEach(
            columnMetadata -> {
              exp.addFilter(eq(columnMetadata));
            });
    return exp;
  }

  public DBLogicalExpression eqAllPartitionKeysAndClusteringKeys(TableMetadata tableMetadata) {
    var exp = new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
    tableMetadata
        .getPrimaryKey()
        .forEach(
            columnMetadata -> {
              exp.addFilter(eq(columnMetadata));
            });
    return exp;
  }

  public DBLogicalExpression eqOnColumnThatIsNotOnSAI(TableMetadata tableMetadata) {
    // Note, this does not contain PartitionKeys and PrimaryKeys
    var firstColumnThatIsNotOnSAI =
        tableMetadata.getColumns().values().stream()
            .filter(
                columnMetadata -> !tableMetadata.getIndexes().containsKey(columnMetadata.getName()))
            .filter(columnMetadata -> !tableMetadata.getPrimaryKey().contains(columnMetadata))
            .findFirst();
    if (firstColumnThatIsNotOnSAI.isEmpty()) {
      throw new IllegalArgumentException(
          "Table don't have a column that is NOT on the SAI table to generate test data");
    }
    var exp = new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
    exp.addFilter(eq(firstColumnThatIsNotOnSAI.get()));
    return exp;
  }

  public DBLogicalExpression eqAllPartitionKeysAndSkippingOneClusteringKey(
      TableMetadata tableMetadata) {
    var exp = new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
    tableMetadata
        .getPartitionKey()
        .forEach(
            columnMetadata -> {
              exp.addFilter(eq(columnMetadata));
            });

    Set<ColumnMetadata> clusteringColumnNames = tableMetadata.getClusteringColumns().keySet();
    if (clusteringColumnNames.size() <= 2) {
      throw new IllegalArgumentException(
          "Target table does not have more than 2 clustering columns.");
    }
    // Retrieve ColumnMetadata for each clustering column
    List<ColumnMetadata> clusteringColumnMetadata = clusteringColumnNames.stream().toList();

    for (int i = 0; i < clusteringColumnMetadata.size(); i++) {
      // Skip the second clustering column (index 1)
      if (i == 1) {
        continue;
      }
      exp.addFilter(eq(clusteringColumnMetadata.get(i)));
    }
    return exp;
  }

  public DBLogicalExpression empty() {
    return new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
  }

  public TableFilter eq(ColumnMetadata columnMetadata) {
    return filter(
        columnMetadata.getName(),
        columnMetadata.getType(),
        NativeTypeTableFilter.Operator.EQ,
        value(columnMetadata.getType()));
  }

  public TableFilter filter(
      CqlIdentifier column, DataType type, NativeTypeTableFilter.Operator operator, Object value) {
    if (type.equals(DataTypes.TEXT)) {
      return new TextTableFilter(column.asInternal(), operator, (String) value);
    }
    throw new IllegalArgumentException("Unsupported type");
  }

  public Object value(DataType type) {
    if (type.equals(DataTypes.TEXT)) {
      return "text-value";
    }
    throw new IllegalArgumentException("Unsupported type");
  }
}
