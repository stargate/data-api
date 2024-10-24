package io.stargate.sgv2.jsonapi.fixtures.testdata;

import static io.stargate.sgv2.jsonapi.fixtures.testdata.LogicalExpressionTestData.ExpressionBuilder.jsonNodeValue;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import io.stargate.sgv2.jsonapi.service.operation.query.ColumnAssignment;
import io.stargate.sgv2.jsonapi.service.operation.query.DBLogicalExpression;
import io.stargate.sgv2.jsonapi.service.shredding.tables.RowShredder;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class UpdateClauseTestData extends TestDataSuplier {

  public UpdateClauseTestData(TestData testData) {
    super(testData);
  }

  public DBLogicalExpression andExpression(TableMetadata tableMetadata) {
    return new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
  }

  public static class ColumnAssignmentsBuilder<FixtureT> {
    public final List<ColumnAssignment> columnAssignments;
    private final TableMetadata tableMetadata;
    private final FixtureT fixture;

    public ColumnAssignmentsBuilder(FixtureT fixture, TableMetadata tableMetadata) {
      this.fixture = fixture;
      this.columnAssignments = new ArrayList<>();
      this.tableMetadata = tableMetadata;
    }

    public FixtureT setOnKnownColumn(CqlIdentifier column) {
      var columnMetadata = tableMetadata.getColumn(column);
      if (columnMetadata.isEmpty()) {
        throw new IllegalArgumentException("Column " + column + " does not exist");
      }

      var columnAssignment =
          new ColumnAssignment(
              tableMetadata,
              column,
              RowShredder.shredValue(jsonNodeValue(columnMetadata.get().getType())));
      columnAssignments.add(columnAssignment);
      return fixture;
    }

    public FixtureT setOnUnknownColumn(CqlIdentifier unknownColumn) {
      var defaultDataType = DataTypes.TEXT;
      var columnAssignment =
          new ColumnAssignment(
              tableMetadata, unknownColumn, RowShredder.shredValue(jsonNodeValue(defaultDataType)));
      columnAssignments.add(columnAssignment);
      return fixture;
    }

    public FixtureT setOnPrimaryKeys() {
      var assignments =
          tableMetadata.getPrimaryKey().stream()
              // Map each primary key column to a new ColumnAssignment
              .map(
                  pk ->
                      new ColumnAssignment(
                          tableMetadata,
                          pk.getName(),
                          RowShredder.shredValue(
                              jsonNodeValue(pk.getType())) // Shred the value based on its type
                          ))
              .toList();

      columnAssignments.addAll(assignments);
      return fixture;
    }

    public FixtureT setOnKnownColumn() {
      var primaryKeyColumnNames =
          tableMetadata.getPrimaryKey().stream()
              .map(ColumnMetadata::getName) // Extract column names
              .collect(Collectors.toSet()); // Store in a set for fast lookup

      var assignments =
          tableMetadata.getColumns().entrySet().stream()
              // Filter out primary key columns by comparing their names
              .filter(column -> !primaryKeyColumnNames.contains(column.getKey()))
              .map(
                  column ->
                      new ColumnAssignment(
                          tableMetadata,
                          column.getKey(),
                          RowShredder.shredValue( // Process the value based on its type
                              jsonNodeValue(
                                  column
                                      .getValue()
                                      .getType()) // Retrieve value for the column's type
                              )))
              .toList(); // Collect the results into a list
      columnAssignments.addAll(assignments);
      return fixture;
    }
  }
}
