package io.stargate.sgv2.jsonapi.util;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultColumnMetadata;
import com.datastax.oss.driver.internal.core.type.DefaultMapType;
import com.datastax.oss.driver.internal.core.type.DefaultSetType;
import com.datastax.oss.driver.internal.core.type.DefaultTupleType;
import com.datastax.oss.driver.internal.core.type.PrimitiveType;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import io.stargate.sgv2.jsonapi.service.cqldriver.override.ExtendedVectorType;
import java.util.List;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ColumnMetadataPredicateTest {

  private static final CqlIdentifier KEYSPACE = CqlIdentifier.fromInternal("keyspace");
  private static final CqlIdentifier TABLE = CqlIdentifier.fromInternal("table");
  private static final CqlIdentifier COLUMN = CqlIdentifier.fromInternal("column");
  private static final CqlIdentifier WRONG = CqlIdentifier.fromInternal("wrong");

  // NOTE: Replicating the previous technique the test used to get the data types
  // for this refactor PR, may will change later.
  private static ColumnMetadata columnMetadata(DataType type) {
    return new DefaultColumnMetadata(KEYSPACE, TABLE, COLUMN, type, false);
  }

  private static ColumnMetadata columnMetadata(int protoTypeCode) {
    // example of where to get the protoTypeCode
    // new PrimitiveType(ProtocolConstants.DataType.VARCHAR)
    return columnMetadata(new PrimitiveType(protoTypeCode));
  }

  @Nested
  class BasicType {

    @Test
    public void happyPath() {
      var columnMetadata = columnMetadata(ProtocolConstants.DataType.VARCHAR);
      var matcher =
          new ColumnMetadataPredicate.BasicType(
              COLUMN, new PrimitiveType(ProtocolConstants.DataType.VARCHAR));

      assertThat(matcher.test(columnMetadata)).isTrue();
    }

    @Test
    public void wrongType() {
      var columnMetadata = columnMetadata(ProtocolConstants.DataType.INT);
      var matcher =
          new ColumnMetadataPredicate.BasicType(
              COLUMN, new PrimitiveType(ProtocolConstants.DataType.VARCHAR));

      assertThat(matcher.test(columnMetadata)).isFalse();
    }

    @Test
    public void notBasicType() {
      var columnMetadata =
          columnMetadata(
              new DefaultMapType(
                  new PrimitiveType(ProtocolConstants.DataType.INT),
                  new PrimitiveType(ProtocolConstants.DataType.INT),
                  false));
      var matcher =
          new ColumnMetadataPredicate.BasicType(
              COLUMN, new PrimitiveType(ProtocolConstants.DataType.VARCHAR));

      assertThat(matcher.test(columnMetadata)).isFalse();
    }

    @Test
    public void wrongName() {
      var columnMetadata = columnMetadata(ProtocolConstants.DataType.VARCHAR);
      var matcher =
          new ColumnMetadataPredicate.BasicType(
              WRONG, new PrimitiveType(ProtocolConstants.DataType.VARCHAR));

      assertThat(matcher.test(columnMetadata)).isFalse();
    }
  }

  @Nested
  class Tuple {

    @Test
    public void happyPath() {
      var columnMetadata =
          columnMetadata(
              new DefaultTupleType(
                  List.of(
                      new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
                      new PrimitiveType(ProtocolConstants.DataType.INT))));
      var matcher =
          new ColumnMetadataPredicate.Tuple(
              COLUMN,
              new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
              new PrimitiveType(ProtocolConstants.DataType.INT));

      assertThat(matcher.test(columnMetadata)).isTrue();
    }

    @Test
    public void wrongOrder() {
      var columnMetadata =
          columnMetadata(
              new DefaultTupleType(
                  List.of(
                      new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
                      new PrimitiveType(ProtocolConstants.DataType.INT))));
      var matcher =
          new ColumnMetadataPredicate.Tuple(
              COLUMN,
              new PrimitiveType(ProtocolConstants.DataType.INT),
              new PrimitiveType(ProtocolConstants.DataType.VARCHAR));

      assertThat(matcher.test(columnMetadata)).isFalse();
    }

    @Test
    public void wrongTuple() {
      var columnMetadata =
          columnMetadata(
              new DefaultTupleType(
                  List.of(
                      new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
                      new PrimitiveType(ProtocolConstants.DataType.INT))));
      var matcher =
          new ColumnMetadataPredicate.Tuple(
              COLUMN, new PrimitiveType(ProtocolConstants.DataType.INT));

      assertThat(matcher.test(columnMetadata)).isFalse();
    }

    @Test
    public void notTuple() {
      var columnMetadata = columnMetadata(ProtocolConstants.DataType.VARCHAR);
      var matcher =
          new ColumnMetadataPredicate.Tuple(
              COLUMN, new PrimitiveType(ProtocolConstants.DataType.INT));

      assertThat(matcher.test(columnMetadata)).isFalse();
    }

    @Test
    public void wrongColumn() {
      var columnMetadata =
          columnMetadata(
              new DefaultTupleType(
                  List.of(
                      new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
                      new PrimitiveType(ProtocolConstants.DataType.INT))));
      var matcher =
          new ColumnMetadataPredicate.Tuple(
              WRONG,
              new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
              new PrimitiveType(ProtocolConstants.DataType.INT));

      assertThat(matcher.test(columnMetadata)).isFalse();
    }
  }

  @Nested
  class Map {

    @Test
    public void happyPath() {
      var columnMetadata =
          columnMetadata(
              new DefaultMapType(
                  new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
                  new PrimitiveType(ProtocolConstants.DataType.INT),
                  false));
      var matcher =
          new ColumnMetadataPredicate.Map(
              COLUMN,
              new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
              new PrimitiveType(ProtocolConstants.DataType.INT));

      assertThat(matcher.test(columnMetadata)).isTrue();
    }

    @Test
    public void wrongValue() {
      var columnMetadata =
          columnMetadata(
              new DefaultMapType(
                  new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
                  new PrimitiveType(ProtocolConstants.DataType.INT),
                  false));
      var matcher =
          new ColumnMetadataPredicate.Map(
              COLUMN,
              new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
              new PrimitiveType(ProtocolConstants.DataType.FLOAT));

      assertThat(matcher.test(columnMetadata)).isFalse();
    }

    @Test
    public void wrongKey() {
      var columnMetadata =
          columnMetadata(
              new DefaultMapType(
                  new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
                  new PrimitiveType(ProtocolConstants.DataType.INT),
                  false));
      var matcher =
          new ColumnMetadataPredicate.Map(
              COLUMN,
              new PrimitiveType(ProtocolConstants.DataType.INT),
              new PrimitiveType(ProtocolConstants.DataType.INT));

      assertThat(matcher.test(columnMetadata)).isFalse();
    }

    @Test
    public void notMap() {
      var columnMetadata = columnMetadata(ProtocolConstants.DataType.VARCHAR);
      var matcher =
          new ColumnMetadataPredicate.Map(
              COLUMN,
              new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
              new PrimitiveType(ProtocolConstants.DataType.INT));

      assertThat(matcher.test(columnMetadata)).isFalse();
    }

    @Test
    public void wrongColumn() {
      var columnMetadata =
          columnMetadata(
              new DefaultMapType(
                  new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
                  new PrimitiveType(ProtocolConstants.DataType.INT),
                  false));
      var matcher =
          new ColumnMetadataPredicate.Map(
              WRONG,
              new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
              new PrimitiveType(ProtocolConstants.DataType.INT));

      assertThat(matcher.test(columnMetadata)).isFalse();
    }
  }

  @Nested
  class Set {

    @Test
    public void happyPath() {
      var columnMetadata =
          columnMetadata(
              new DefaultSetType(new PrimitiveType(ProtocolConstants.DataType.VARCHAR), false));
      var matcher =
          new ColumnMetadataPredicate.Set(
              COLUMN, new PrimitiveType(ProtocolConstants.DataType.VARCHAR));

      assertThat(matcher.test(columnMetadata)).isTrue();
    }

    @Test
    public void wrongType() {
      var columnMetadata =
          columnMetadata(
              new DefaultSetType(new PrimitiveType(ProtocolConstants.DataType.VARCHAR), false));
      var matcher =
          new ColumnMetadataPredicate.Set(COLUMN, new PrimitiveType(ProtocolConstants.DataType.INT));

      assertThat(matcher.test(columnMetadata)).isFalse();
    }

    @Test
    public void notSet() {
      var columnMetadata = columnMetadata(ProtocolConstants.DataType.VARCHAR);
      var matcher =
          new ColumnMetadataPredicate.Set(COLUMN, new PrimitiveType(ProtocolConstants.DataType.INT));

      assertThat(matcher.test(columnMetadata)).isFalse();
    }

    @Test
    public void wrongColumn() {
      var columnMetadata =
          columnMetadata(
              new DefaultSetType(new PrimitiveType(ProtocolConstants.DataType.VARCHAR), false));
      var matcher =
          new ColumnMetadataPredicate.Set(
              WRONG, new PrimitiveType(ProtocolConstants.DataType.VARCHAR));

      assertThat(matcher.test(columnMetadata)).isFalse();
    }
  }

  @Nested
  class Vector {
    @Test
    public void happyPath() {
      var columnMetadata =
          columnMetadata(
              new ExtendedVectorType(new PrimitiveType(ProtocolConstants.DataType.FLOAT), 1024));
      var matcher =
          new ColumnMetadataPredicate.Vector(
              COLUMN, new PrimitiveType(ProtocolConstants.DataType.FLOAT));

      assertThat(matcher.test(columnMetadata)).isTrue();
    }

    @Test
    public void wrongType() {
      var columnMetadata =
          columnMetadata(
              new ExtendedVectorType(new PrimitiveType(ProtocolConstants.DataType.INT), 1024));
      var matcher =
          new ColumnMetadataPredicate.Vector(
              COLUMN, new PrimitiveType(ProtocolConstants.DataType.FLOAT));

      assertThat(matcher.test(columnMetadata)).isFalse();
    }

    @Test
    public void notVector() {
      var columnMetadata = columnMetadata(ProtocolConstants.DataType.VARCHAR);
      var matcher =
          new ColumnMetadataPredicate.Vector(
              COLUMN, new PrimitiveType(ProtocolConstants.DataType.FLOAT));

      assertThat(matcher.test(columnMetadata)).isFalse();
    }

    @Test
    public void wrongColumn() {
      var columnMetadata =
          columnMetadata(
              new ExtendedVectorType(new PrimitiveType(ProtocolConstants.DataType.FLOAT), 1024));
      var matcher =
          new ColumnMetadataPredicate.Vector(
              WRONG, new PrimitiveType(ProtocolConstants.DataType.FLOAT));

      assertThat(matcher.test(columnMetadata)).isFalse();
    }
  }
}
