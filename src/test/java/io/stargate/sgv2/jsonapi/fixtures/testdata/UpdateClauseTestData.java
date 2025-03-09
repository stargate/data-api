package io.stargate.sgv2.jsonapi.fixtures.testdata;

import static io.stargate.sgv2.jsonapi.fixtures.testdata.LogicalExpressionTestData.ExpressionBuilder.jsonNodeValue;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToJsonKey;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.UpdateException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistries;
import io.stargate.sgv2.jsonapi.service.operation.query.ColumnAssignment;
import io.stargate.sgv2.jsonapi.service.operation.query.ColumnSetToAssignment;
import io.stargate.sgv2.jsonapi.service.operation.query.DBLogicalExpression;
import io.stargate.sgv2.jsonapi.service.shredding.CqlNamedValue;
import io.stargate.sgv2.jsonapi.service.shredding.CqlNamedValueContainer;
import io.stargate.sgv2.jsonapi.service.shredding.JsonNodeDecoder;
import io.stargate.sgv2.jsonapi.service.shredding.tables.CqlNamedValueFactory;
import io.stargate.sgv2.jsonapi.service.shredding.tables.JsonNamedValueFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class UpdateClauseTestData extends TestDataSuplier {

  private static final CqlNamedValue.ErrorStrategy<UpdateException> THROW_ALL_ERROR_STRATEGY =
      new CqlNamedValue.ErrorStrategy<>() {

        @Override
        public void allChecks(
            TableSchemaObject tableSchemaObject, CqlNamedValueContainer allColumns) {
          // skipping check for support of the operation type because we dont have one here
          checkCodecError(tableSchemaObject, allColumns);
          checkMissingCodec(tableSchemaObject, allColumns);
          checkUnknownColumns(tableSchemaObject, allColumns);
        }

        @Override
        public ErrorCode<UpdateException> codeForNoApiSupport() {
          throw new UnsupportedOperationException("codeForNoApiSupport Not implemented");
        }

        @Override
        public ErrorCode<UpdateException> codeForUnknownColumn() {
          return UpdateException.Code.UNKNOWN_TABLE_COLUMNS;
        }

        @Override
        public ErrorCode<UpdateException> codeForMissingCodec() {
          throw new UnsupportedOperationException("codeForMissingCodec Not implemented");
        }

        @Override
        public ErrorCode<UpdateException> codeForCodecError() {
          throw new UnsupportedOperationException("codeForCodecError Not implemented");
        }
      };

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

    private ColumnAssignment buildColumnAssignment(
        TableSchemaObject tableSchemaObject,
        CqlIdentifier column,
        Function<CqlNamedValue, ColumnAssignment> assignmentSupplier) {
      var columnMetadata = tableMetadata.getColumn(column);
      if (columnMetadata.isEmpty()) {
        throw new IllegalArgumentException("Column " + column + " does not exist");
      }
      return buildColumnAssignment(
          tableSchemaObject,
          column,
          jsonNodeValue(columnMetadata.get().getType()),
          assignmentSupplier);
    }

    private ColumnAssignment buildColumnAssignment(
        TableSchemaObject tableSchemaObject,
        CqlIdentifier column,
        JsonNode value,
        Function<CqlNamedValue, ColumnAssignment> assignmentSupplier) {

      var objectMapper = new ObjectMapper();
      var node = objectMapper.createObjectNode().set(cqlIdentifierToJsonKey(column), value);

      var jsonNamedValues =
          new JsonNamedValueFactory(tableSchemaObject, JsonNodeDecoder.DEFAULT).create(node);
      var cqlNamedValues =
          new CqlNamedValueFactory(
                  tableSchemaObject, JSONCodecRegistries.DEFAULT_REGISTRY, THROW_ALL_ERROR_STRATEGY)
              .create(jsonNamedValues);
      assert cqlNamedValues.size() == 1;

      return assignmentSupplier.apply(cqlNamedValues.values().iterator().next());
    }

    public FixtureT setOnKnownColumn(TableSchemaObject tableSchemaObject, CqlIdentifier column) {
      columnAssignments.add(
          buildColumnAssignment(tableSchemaObject, column, ColumnSetToAssignment::new));
      return fixture;
    }

    public FixtureT setOnUnknownColumn(
        TableSchemaObject tableSchemaObject, CqlIdentifier unknownColumn) {

      // data type does not matter, ok to always use text
      columnAssignments.add(
          buildColumnAssignment(
              tableSchemaObject,
              unknownColumn,
              jsonNodeValue(DataTypes.TEXT),
              ColumnSetToAssignment::new));
      return fixture;
    }

    public FixtureT setOnPrimaryKeys(TableSchemaObject tableSchemaObject) {
      var assignments =
          tableMetadata.getPrimaryKey().stream()
              // Map each primary key column to a new ColumnAssignment
              .map(
                  pk ->
                      buildColumnAssignment(
                          tableSchemaObject,
                          pk.getName(),
                          jsonNodeValue(pk.getType()),
                          ColumnSetToAssignment::new))
              .toList();

      columnAssignments.addAll(assignments);
      return fixture;
    }

    public FixtureT setOnKnownColumn(TableSchemaObject tableSchemaObject) {

      var primaryKeyColumnNames =
          tableMetadata.getPrimaryKey().stream()
              .map(ColumnMetadata::getName)
              .collect(Collectors.toSet());

      var assignments =
          tableMetadata.getColumns().entrySet().stream()
              // Filter out primary key columns by comparing their names
              .filter(entry -> !primaryKeyColumnNames.contains(entry.getKey()))
              .map(
                  entry ->
                      buildColumnAssignment(
                          tableSchemaObject,
                          entry.getKey(),
                          jsonNodeValue(entry.getValue().getType()),
                          ColumnSetToAssignment::new))
              .toList(); // Collect the results into a list
      columnAssignments.addAll(assignments);
      return fixture;
    }
  }
}
