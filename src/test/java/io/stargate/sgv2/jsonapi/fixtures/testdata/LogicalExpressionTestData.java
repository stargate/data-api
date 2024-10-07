package io.stargate.sgv2.jsonapi.fixtures.testdata;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.NativeTypeTableFilter;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.TextTableFilter;
import io.stargate.sgv2.jsonapi.service.operation.query.DBLogicalExpression;
import io.stargate.sgv2.jsonapi.service.operation.query.TableFilter;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import java.util.stream.Collectors;

public class LogicalExpressionTestData extends TestDataSuplier {

  public LogicalExpressionTestData(TestData testData) {
    super(testData);
  }

  public DBLogicalExpression andExpression(TableMetadata tableMetadata) {
    return new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
  }

  public static class ExpressionBuilder<FixtureT> {
    public final DBLogicalExpression expression;
    private final TableMetadata tableMetadata;
    private final FixtureT fixture;

    public ExpressionBuilder(
        FixtureT fixture, DBLogicalExpression expression, TableMetadata tableMetadata) {
      this.fixture = fixture;
      this.expression = expression;
      this.tableMetadata = tableMetadata;
    }

    public FixtureT eqOn(CqlIdentifier column) {
      expression.addFilter(eq(tableMetadata.getColumn(column).orElseThrow()));
      return fixture;
    }

    public FixtureT notEqOn(CqlIdentifier column) {
      expression.addFilter(notEq(tableMetadata.getColumn(column).orElseThrow()));
      return fixture;
    }

    public FixtureT gtOn(CqlIdentifier column) {
      expression.addFilter(gt(tableMetadata.getColumn(column).orElseThrow()));
      return fixture;
    }

    public FixtureT eqAllPrimaryKeys() {
      eqAllPartitionKeys();
      return eqAllClusteringKeys();
    }

    public FixtureT eqAllPartitionKeys() {
      tableMetadata
          .getPartitionKey()
          .forEach(
              columnMetadata -> {
                expression.addFilter(eq(columnMetadata));
              });
      return fixture;
    }

    public FixtureT eqAllClusteringKeys() {
      tableMetadata
          .getClusteringColumns()
          .keySet()
          .forEach(
              columnMetadata -> {
                expression.addFilter(eq(columnMetadata));
              });
      return fixture;
    }

    public FixtureT eqSkipOneClusteringKeys(int skipIndex) {
      int index = -1;
      for (ColumnMetadata columnMetadata : tableMetadata.getClusteringColumns().keySet()) {
        index++;
        if (index == skipIndex) {
          continue;
        }
        expression.addFilter(eq(columnMetadata));
      }
      return fixture;
    }

    public FixtureT eqOnlyOneClusteringKey(int index) {

      ColumnMetadata columnMetadata =
          tableMetadata.getClusteringColumns().keySet().stream().toList().get(index);
      expression.addFilter(eq(columnMetadata));
      return fixture;
    }

    public FixtureT eqFirstNonPKOrIndexed() {
      // Indexes are keyed on the index name, not the indexed field.
      var allIndexTargets =
          tableMetadata.getIndexes().values().stream()
              .map(IndexMetadata::getTarget)
              .map(CqlIdentifierUtil::cqlIdentifierFromIndexTarget)
              .collect(Collectors.toSet());

      tableMetadata.getColumns().values().stream()
          .filter(columnMetadata -> !tableMetadata.getPrimaryKey().contains(columnMetadata))
          .filter(columnMetadata -> !allIndexTargets.contains(columnMetadata.getName()))
          .findFirst()
          .ifPresentOrElse(
              columnMetadata -> expression.addFilter(eq(columnMetadata)),
              () -> {
                throw new IllegalArgumentException(
                    "Table don't have a column that is NOT on the SAI table to generate test data");
              });
      return fixture;
    }

    public static TableFilter eq(ColumnMetadata columnMetadata) {
      return filter(
          columnMetadata.getName(),
          columnMetadata.getType(),
          NativeTypeTableFilter.Operator.EQ,
          value(columnMetadata.getType()));
    }

    public static TableFilter notEq(ColumnMetadata columnMetadata) {
      return filter(
          columnMetadata.getName(),
          columnMetadata.getType(),
          NativeTypeTableFilter.Operator.NE,
          value(columnMetadata.getType()));
    }

    public static TableFilter gt(ColumnMetadata columnMetadata) {
      return filter(
          columnMetadata.getName(),
          columnMetadata.getType(),
          NativeTypeTableFilter.Operator.GT,
          value(columnMetadata.getType()));
    }

    public static TableFilter filter(
        CqlIdentifier column,
        DataType type,
        NativeTypeTableFilter.Operator operator,
        Object value) {
      if (type.equals(DataTypes.TEXT)) {
        return new TextTableFilter(column.asInternal(), operator, (String) value);
      }
      if (type.equals(DataTypes.DURATION)) {
        // we pass a string to the codec for a duration
        return new TextTableFilter(column.asInternal(), operator, (String) value);
      }
      throw new IllegalArgumentException("Unsupported type");
    }

    public static Object value(DataType type) {
      if (type.equals(DataTypes.TEXT)) {
        return "text-value";
      }
      if (type.equals(DataTypes.DURATION)) {
        // we handle duration as a string until it gets to the codec
        return "P1H30M";
      }
      throw new IllegalArgumentException("Unsupported type");
    }
  }
}
