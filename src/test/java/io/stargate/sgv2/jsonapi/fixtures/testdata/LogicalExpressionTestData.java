package io.stargate.sgv2.jsonapi.fixtures.testdata;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import io.stargate.sgv2.jsonapi.exception.catchable.MissingJSONCodecException;
import io.stargate.sgv2.jsonapi.exception.catchable.ToCQLCodecException;
import io.stargate.sgv2.jsonapi.exception.catchable.UnknownColumnException;
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

      throw new IllegalArgumentException("Unsupported type");
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
