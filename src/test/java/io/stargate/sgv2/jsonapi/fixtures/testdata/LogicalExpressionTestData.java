package io.stargate.sgv2.jsonapi.fixtures.testdata;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.internal.core.type.DefaultListType;
import com.datastax.oss.driver.internal.core.type.DefaultMapType;
import com.datastax.oss.driver.internal.core.type.DefaultSetType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.exception.checked.MissingJSONCodecException;
import io.stargate.sgv2.jsonapi.exception.checked.ToCQLCodecException;
import io.stargate.sgv2.jsonapi.exception.checked.UnknownColumnException;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.*;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodec;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistries;
import io.stargate.sgv2.jsonapi.service.operation.query.DBLogicalExpression;
import io.stargate.sgv2.jsonapi.service.operation.query.TableFilter;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogicalExpressionTestData extends TestDataSuplier {

  private static final Logger log = LoggerFactory.getLogger(LogicalExpressionTestData.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public LogicalExpressionTestData(TestData testData) {
    super(testData);
  }

  public DBLogicalExpression implicitAndExpression(TableMetadata tableMetadata) {
    return new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
  }

  public static class ExpressionBuilder<FixtureT> {
    public DBLogicalExpression rootImplicitAnd;
    private final TableMetadata tableMetadata;
    private final FixtureT fixture;

    public ExpressionBuilder(
        FixtureT fixture, DBLogicalExpression rootImplicitAnd, TableMetadata tableMetadata) {
      this.fixture = fixture;
      this.rootImplicitAnd = rootImplicitAnd;
      this.tableMetadata = tableMetadata;
    }

    public FixtureT replaceRootDBLogicalExpression(DBLogicalExpression dbLogicalExpression) {
      this.rootImplicitAnd = dbLogicalExpression;
      return fixture;
    }

    public FixtureT eqOn(CqlIdentifier column) {
      rootImplicitAnd.addFilter(eq(tableMetadata.getColumn(column).orElseThrow()));
      return fixture;
    }

    public FixtureT notEqOn(CqlIdentifier column) {
      rootImplicitAnd.addFilter(notEq(tableMetadata.getColumn(column).orElseThrow()));
      return fixture;
    }

    public FixtureT gtOn(CqlIdentifier column) {
      rootImplicitAnd.addFilter(gt(tableMetadata.getColumn(column).orElseThrow()));
      return fixture;
    }

    public FixtureT inOn(CqlIdentifier column) {
      rootImplicitAnd.addFilter(in(tableMetadata.getColumn(column).orElseThrow()));
      return fixture;
    }

    public FixtureT notInOn(CqlIdentifier column) {
      rootImplicitAnd.addFilter(nin(tableMetadata.getColumn(column).orElseThrow()));
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
                rootImplicitAnd.addFilter(eq(columnMetadata));
              });
      return fixture;
    }

    public FixtureT eqSkipOnePartitionKeys(int skipIndex) {
      if (skipIndex >= tableMetadata.getPartitionKey().size()) {
        throw new IllegalArgumentException(
            "Skip index is greater than the number of partition keys");
      }
      int index = -1;
      for (ColumnMetadata columnMetadata : tableMetadata.getPartitionKey()) {
        index++;
        if (index == skipIndex) {
          continue;
        }
        rootImplicitAnd.addFilter(eq(columnMetadata));
      }
      return fixture;
    }

    public FixtureT eqAllClusteringKeys() {
      tableMetadata
          .getClusteringColumns()
          .keySet()
          .forEach(
              columnMetadata -> {
                rootImplicitAnd.addFilter(eq(columnMetadata));
              });
      return fixture;
    }

    public FixtureT inOnOnePartitionKey(
        InTableFilter.Operator inFilterOperator, ColumnMetadata firstPartitionKey) {
      if (inFilterOperator == InTableFilter.Operator.IN) {
        rootImplicitAnd.addFilter(in(firstPartitionKey));
      }
      if (inFilterOperator == InTableFilter.Operator.NIN) {
        rootImplicitAnd.addFilter(nin(firstPartitionKey));
      }
      return fixture;
    }

    public FixtureT eqSkipOneClusteringKeys(int skipIndex) {

      if (skipIndex >= tableMetadata.getClusteringColumns().size()) {
        throw new IllegalArgumentException(
            "Skip index is greater than the number of clustering keys");
      }
      int index = -1;
      for (ColumnMetadata columnMetadata : tableMetadata.getClusteringColumns().keySet()) {
        index++;
        if (index == skipIndex) {
          continue;
        }
        rootImplicitAnd.addFilter(eq(columnMetadata));
      }
      return fixture;
    }

    public FixtureT eqOnlyOneClusteringKey(int index) {

      ColumnMetadata columnMetadata =
          tableMetadata.getClusteringColumns().keySet().stream().toList().get(index);
      rootImplicitAnd.addFilter(eq(columnMetadata));
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
              columnMetadata -> rootImplicitAnd.addFilter(eq(columnMetadata)),
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

    public TableFilter eq(CqlIdentifier columnCqlIdentifier) {
      return eq(tableMetadata.getColumn(columnCqlIdentifier).orElseThrow());
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

    public static TableFilter in(ColumnMetadata columnMetadata) {
      return new InTableFilter(
          InTableFilter.Operator.IN,
          columnMetadata.getName().asInternal(),
          List.of(value(columnMetadata.getType()), value(columnMetadata.getType())));
    }

    public static TableFilter nin(ColumnMetadata columnMetadata) {
      return new InTableFilter(
          InTableFilter.Operator.NIN,
          columnMetadata.getName().asInternal(),
          List.of(value(columnMetadata.getType()), value(columnMetadata.getType())));
    }

    /**
     * Creates the table filter for map/set/list column. Caller should specify the operator and the
     * MapSetListComponent. Method will create the filter based on the column type and the
     * component, and also populate the positionalValues.
     */
    public TableFilter filterOnMapSetList(
        CqlIdentifier columnCqlIdentifier,
        MapSetListTableFilter.Operator operator,
        MapSetListTableFilter.MapSetListFilterComponent mapSetListComponent) {
      var columnMetadata = tableMetadata.getColumn(columnCqlIdentifier).get();
      return switch (mapSetListComponent) {
        case LIST_VALUE:
          {
            DefaultListType listType = (DefaultListType) columnMetadata.getType();
            yield new MapSetListTableFilter(
                operator,
                columnMetadata.getName().asInternal(),
                List.of(value(listType.getElementType()), value(listType.getElementType())),
                MapSetListTableFilter.MapSetListFilterComponent.LIST_VALUE);
          }
        case SET_VALUE:
          {
            DefaultSetType setType = (DefaultSetType) columnMetadata.getType();
            yield new MapSetListTableFilter(
                operator,
                columnMetadata.getName().asInternal(),
                List.of(value(setType.getElementType()), value(setType.getElementType())),
                MapSetListTableFilter.MapSetListFilterComponent.SET_VALUE);
          }
        case MAP_KEY:
          {
            DefaultMapType mapType = (DefaultMapType) columnMetadata.getType();
            yield new MapSetListTableFilter(
                operator,
                columnMetadata.getName().asInternal(),
                List.of(value(mapType.getKeyType()), value(mapType.getKeyType())),
                MapSetListTableFilter.MapSetListFilterComponent.MAP_KEY);
          }
        case MAP_VALUE:
          {
            DefaultMapType mapType = (DefaultMapType) columnMetadata.getType();
            yield new MapSetListTableFilter(
                operator,
                columnMetadata.getName().asInternal(),
                List.of(value(mapType.getValueType()), value(mapType.getValueType())),
                MapSetListTableFilter.MapSetListFilterComponent.MAP_VALUE);
          }
        case MAP_ENTRY:
          {
            DefaultMapType mapType = (DefaultMapType) columnMetadata.getType();
            yield new MapSetListTableFilter(
                operator,
                columnMetadata.getName().asInternal(),
                List.of(
                    List.of(value(mapType.getKeyType()), value(mapType.getValueType())),
                    List.of(value(mapType.getKeyType()), value(mapType.getValueType()))),
                MapSetListTableFilter.MapSetListFilterComponent.MAP_ENTRY);
          }
      };
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
        return new TextTableFilter(column.asInternal(), operator, (String) value);
      }
      if (type.equals(DataTypes.INT)) {
        return new NumberTableFilter(column.asInternal(), operator, (Number) value);
      }
      if (type.equals(DataTypes.BIGINT)) {
        return new NumberTableFilter(column.asInternal(), operator, (Number) value);
      }
      if (type.equals(DataTypes.DECIMAL)) {
        return new NumberTableFilter(column.asInternal(), operator, (Number) value);
      }
      if (type.equals(DataTypes.DOUBLE)) {
        return new NumberTableFilter(column.asInternal(), operator, (Number) value);
      }
      if (type.equals(DataTypes.FLOAT)) {
        return new NumberTableFilter(column.asInternal(), operator, (Number) value);
      }
      if (type.equals(DataTypes.SMALLINT)) {
        return new NumberTableFilter(column.asInternal(), operator, (Number) value);
      }
      if (type.equals(DataTypes.TINYINT)) {
        return new NumberTableFilter(column.asInternal(), operator, (Number) value);
      }
      if (type.equals(DataTypes.VARINT)) {
        return new NumberTableFilter(column.asInternal(), operator, (Number) value);
      }
      if (type.equals(DataTypes.BOOLEAN)) {
        return new BooleanTableFilter(column.asInternal(), operator, (Boolean) value);
      }
      if (type.equals(DataTypes.ASCII)) {
        return new TextTableFilter(column.asInternal(), operator, (String) value);
      }
      //      if (type.equals(DataTypes.BLOB)) {
      //        return new BlobTableFilter(column.asInternal(), operator, (String) value);  //
      // Assuming blob is passed as base64 encoded string
      //      }
      if (type.equals(DataTypes.DATE)) {
        return new TextTableFilter(column.asInternal(), operator, (String) value);
      }
      if (type.equals(DataTypes.TIME)) {
        return new TextTableFilter(column.asInternal(), operator, (String) value);
      }
      if (type.equals(DataTypes.TIMESTAMP)) {
        return new TextTableFilter(column.asInternal(), operator, (String) value);
      }
      if (type.equals(DataTypes.INET)) {
        return new TextTableFilter(column.asInternal(), operator, (String) value);
      }
      if (type.equals(DataTypes.UUID)) {
        return new TextTableFilter(column.asInternal(), operator, (String) value);
      }
      if (type.equals(DataTypes.TIMEUUID)) {
        return new TextTableFilter(column.asInternal(), operator, (String) value);
      }

      // Sample Collection type values
      // TODO, note tables feature does not support complex type now, the unit tests just mimic an
      // invalid filter
      // usage against complex datatype columns
      if (type.equals(DataTypes.setOf(DataTypes.TEXT))) {
        return new TextTableFilter(column.asInternal(), operator, (String) value);
      }
      if (type.equals(DataTypes.mapOf(DataTypes.TEXT, DataTypes.TEXT))) {
        return new TextTableFilter(column.asInternal(), operator, (String) value);
      }
      if (type.equals(DataTypes.listOf(DataTypes.TEXT))) {
        return new TextTableFilter(column.asInternal(), operator, (String) value);
      }
      if (type.equals(DataTypes.vectorOf(DataTypes.FLOAT, 3))) {
        return new TextTableFilter(column.asInternal(), operator, (String) value);
      }

      throw new IllegalArgumentException("Unsupported type");
    }

    public static Object value(DataType type) {
      if (type.equals(DataTypes.TEXT)) {
        return "text-value";
      }
      if (type.equals(DataTypes.DURATION)) {
        return "P1H30M"; // Handle duration as a string
      }
      if (type.equals(DataTypes.INT)) {
        return 25L;
      }
      if (type.equals(DataTypes.BIGINT)) {
        return 1000000L;
      }
      if (type.equals(DataTypes.DECIMAL)) {
        return BigDecimal.valueOf(19.99);
      }
      if (type.equals(DataTypes.DOUBLE)) {
        return 4.5d;
      }
      if (type.equals(DataTypes.FLOAT)) {
        return 70.5f;
      }
      if (type.equals(DataTypes.SMALLINT)) {
        return (short) 3;
      }
      if (type.equals(DataTypes.TINYINT)) {
        return (byte) 1;
      }
      if (type.equals(DataTypes.VARINT)) {
        return BigInteger.valueOf(123456789);
      }
      if (type.equals(DataTypes.BOOLEAN)) {
        return true;
      }
      if (type.equals(DataTypes.ASCII)) {
        return "Sample ASCII Text";
      }
      if (type.equals(DataTypes.BLOB)) {
        return "aGVsbG8gd29ybGQ="; // Base64 encoded "hello world"
      }
      if (type.equals(DataTypes.DATE)) {
        return "2024-09-24"; // Sample date
      }
      if (type.equals(DataTypes.TIME)) {
        return "12:45:01.005"; // Sample time
      }
      if (type.equals(DataTypes.TIMESTAMP)) {
        return "2024-09-24T14:06:59Z"; // Sample timestamp
      }
      if (type.equals(DataTypes.INET)) {
        return "127.0.0.1"; // Sample internet address
      }
      if (type.equals(DataTypes.UUID)) {
        return "123e4567-e89b-12d3-a456-426614174000"; // Sample UUID
      }
      if (type.equals(DataTypes.TIMEUUID)) {
        return "123e4567-e89b-12d3-a456-426655440000"; // Sample TIMEUUID
      }

      if (type.equals(DataTypes.TIMEUUID)) {
        return "123e4567-e89b-12d3-a456-426655440000"; // Sample TIMEUUID
      }

      // Sample Collection type values
      // TODO, note tables feature does not support complex type now, the unit tests just mimic an
      // invalid filter
      // usage against complex datatype columns
      if (type.equals(DataTypes.setOf(DataTypes.TEXT))) {
        return null;
      }
      if (type.equals(DataTypes.mapOf(DataTypes.TEXT, DataTypes.TEXT))) {
        return null;
      }
      if (type.equals(DataTypes.listOf(DataTypes.TEXT))) {
        return null;
      }
      if (type.equals(DataTypes.vectorOf(DataTypes.FLOAT, 3))) {
        return null;
      }
      throw new IllegalArgumentException("Unsupported type");
    }

    public static JsonNode jsonNodeValue(DataType dataType) {
      Object value = value(dataType);
      if (value instanceof String) {
        return MAPPER.getNodeFactory().textNode((String) value);
      }
      if (value instanceof Integer) {
        return MAPPER.getNodeFactory().numberNode((Integer) value);
      }
      if (value instanceof Long) {
        return MAPPER.getNodeFactory().numberNode((Long) value);
      }
      if (value instanceof BigDecimal) {
        return MAPPER.getNodeFactory().numberNode((BigDecimal) value);
      }
      if (value instanceof Double) {
        return MAPPER.getNodeFactory().numberNode((Double) value);
      }
      if (value instanceof Float) {
        return MAPPER.getNodeFactory().numberNode((Float) value);
      }
      if (value instanceof Short) {
        return MAPPER.getNodeFactory().numberNode((Short) value);
      }
      if (value instanceof Byte) {
        return MAPPER.getNodeFactory().numberNode((Byte) value);
      }
      if (value instanceof BigInteger) {
        return MAPPER.getNodeFactory().numberNode((BigInteger) value);
      }
      if (value instanceof Boolean) {
        return MAPPER.getNodeFactory().booleanNode((Boolean) value);
      }
      if (value instanceof byte[]) {
        return MAPPER.getNodeFactory().binaryNode((byte[]) value);
      }

      throw new IllegalArgumentException(
          "Did not understand type %s to convert into JsonNode".formatted(dataType));
    }

    // Get CqlValue of type that Driver expects
    public Object CqlValue(CqlIdentifier column) {
      var cqlDataType = tableMetadata.getColumn(column).orElseThrow().getType();
      var javaValue = value(cqlDataType);
      JSONCodec codec = null;
      try {
        codec =
            JSONCodecRegistries.DEFAULT_REGISTRY.codecToCQL(
                tableMetadata,
                column,
                ExpressionBuilder.value(tableMetadata.getColumn(column).orElseThrow().getType()));
      } catch (UnknownColumnException | ToCQLCodecException | MissingJSONCodecException e) {
        throw new IllegalArgumentException(e);
      }
      try {
        return codec.toCQL(javaValue);
      } catch (ToCQLCodecException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
