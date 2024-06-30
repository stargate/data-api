package io.stargate.sgv2.jsonapi.service.schema.model;

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
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class CqlColumnMatcherTest {

  @Nested
  class BasicType {

    @Test
    public void happyPath() {
      ColumnMetadata spec =
          new DefaultColumnMetadata(
              CqlIdentifier.fromInternal("keyspace"),
              CqlIdentifier.fromInternal("collection"),
              CqlIdentifier.fromInternal("column"),
              new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
              false);

      CqlColumnMatcher.BasicType matcher =
          new CqlColumnMatcher.BasicType(
              CqlIdentifier.fromInternal("column"),
              new PrimitiveType(ProtocolConstants.DataType.VARCHAR));
      boolean result = matcher.test(spec);

      assertThat(result).isTrue();
    }

    @Test
    public void wrongType() {
      ColumnMetadata spec =
          new DefaultColumnMetadata(
              CqlIdentifier.fromInternal("keyspace"),
              CqlIdentifier.fromInternal("collection"),
              CqlIdentifier.fromInternal("column"),
              new PrimitiveType(ProtocolConstants.DataType.INT),
              false);

      CqlColumnMatcher.BasicType matcher =
          new CqlColumnMatcher.BasicType(
              CqlIdentifier.fromInternal("column"),
              new PrimitiveType(ProtocolConstants.DataType.VARCHAR));
      boolean result = matcher.test(spec);

      assertThat(result).isFalse();
    }

    @Test
    public void notBasicType() {
      ColumnMetadata spec =
          new DefaultColumnMetadata(
              CqlIdentifier.fromInternal("keyspace"),
              CqlIdentifier.fromInternal("collection"),
              CqlIdentifier.fromInternal("column"),
              new DefaultMapType(
                  new PrimitiveType(ProtocolConstants.DataType.INT),
                  new PrimitiveType(ProtocolConstants.DataType.INT),
                  false),
              false);

      CqlColumnMatcher.BasicType matcher =
          new CqlColumnMatcher.BasicType(
              CqlIdentifier.fromInternal("column"),
              new PrimitiveType(ProtocolConstants.DataType.VARCHAR));
      boolean result = matcher.test(spec);

      assertThat(result).isFalse();
    }

    @Test
    public void wrongName() {
      ColumnMetadata spec =
          new DefaultColumnMetadata(
              CqlIdentifier.fromInternal("keyspace"),
              CqlIdentifier.fromInternal("collection"),
              CqlIdentifier.fromInternal("column"),
              new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
              false);

      CqlColumnMatcher.BasicType matcher =
          new CqlColumnMatcher.BasicType(
              CqlIdentifier.fromInternal("wrong"),
              new PrimitiveType(ProtocolConstants.DataType.VARCHAR));
      boolean result = matcher.test(spec);

      assertThat(result).isFalse();
    }
  }

  @Nested
  class Tuple {

    @Test
    public void happyPath() {
      DataType type1 = new PrimitiveType(ProtocolConstants.DataType.VARCHAR);
      DataType type2 = new PrimitiveType(ProtocolConstants.DataType.INT);
      List<DataType> list = Arrays.asList(type1, type2);
      ColumnMetadata spec =
          new DefaultColumnMetadata(
              CqlIdentifier.fromInternal("keyspace"),
              CqlIdentifier.fromInternal("collection"),
              CqlIdentifier.fromInternal("column"),
              new DefaultTupleType(list),
              false);

      CqlColumnMatcher.Tuple matcher =
          new CqlColumnMatcher.Tuple(
              CqlIdentifier.fromInternal("column"),
              new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
              new PrimitiveType(ProtocolConstants.DataType.INT));
      boolean result = matcher.test(spec);

      assertThat(result).isTrue();
    }

    @Test
    public void wrongOrder() {
      DataType type1 = new PrimitiveType(ProtocolConstants.DataType.VARCHAR);
      DataType type2 = new PrimitiveType(ProtocolConstants.DataType.INT);
      List<DataType> list = Arrays.asList(type1, type2);
      ColumnMetadata spec =
          new DefaultColumnMetadata(
              CqlIdentifier.fromInternal("keyspace"),
              CqlIdentifier.fromInternal("collection"),
              CqlIdentifier.fromInternal("column"),
              new DefaultTupleType(list),
              false);

      CqlColumnMatcher.Tuple matcher =
          new CqlColumnMatcher.Tuple(
              CqlIdentifier.fromInternal("column"),
              new PrimitiveType(ProtocolConstants.DataType.INT),
              new PrimitiveType(ProtocolConstants.DataType.VARCHAR));
      boolean result = matcher.test(spec);

      assertThat(result).isFalse();
    }

    @Test
    public void wrongTuple() {
      DataType type1 = new PrimitiveType(ProtocolConstants.DataType.VARCHAR);
      DataType type2 = new PrimitiveType(ProtocolConstants.DataType.INT);
      List<DataType> list = Arrays.asList(type1, type2);
      ColumnMetadata spec =
          new DefaultColumnMetadata(
              CqlIdentifier.fromInternal("keyspace"),
              CqlIdentifier.fromInternal("collection"),
              CqlIdentifier.fromInternal("column"),
              new DefaultTupleType(list),
              false);

      CqlColumnMatcher.Tuple matcher =
          new CqlColumnMatcher.Tuple(
              CqlIdentifier.fromInternal("column"),
              new PrimitiveType(ProtocolConstants.DataType.INT));
      boolean result = matcher.test(spec);

      assertThat(result).isFalse();
    }

    @Test
    public void notTuple() {
      ColumnMetadata spec =
          new DefaultColumnMetadata(
              CqlIdentifier.fromInternal("keyspace"),
              CqlIdentifier.fromInternal("collection"),
              CqlIdentifier.fromInternal("column"),
              new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
              false);

      CqlColumnMatcher.Tuple matcher =
          new CqlColumnMatcher.Tuple(
              CqlIdentifier.fromInternal("column"),
              new PrimitiveType(ProtocolConstants.DataType.INT));
      boolean result = matcher.test(spec);

      assertThat(result).isFalse();
    }

    @Test
    public void wrongColumn() {
      DataType type1 = new PrimitiveType(ProtocolConstants.DataType.VARCHAR);
      DataType type2 = new PrimitiveType(ProtocolConstants.DataType.INT);
      List<DataType> list = Arrays.asList(type1, type2);
      ColumnMetadata spec =
          new DefaultColumnMetadata(
              CqlIdentifier.fromInternal("keyspace"),
              CqlIdentifier.fromInternal("collection"),
              CqlIdentifier.fromInternal("column"),
              new DefaultTupleType(list),
              false);

      CqlColumnMatcher.Tuple matcher =
          new CqlColumnMatcher.Tuple(
              CqlIdentifier.fromInternal("wrong"),
              new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
              new PrimitiveType(ProtocolConstants.DataType.INT));
      boolean result = matcher.test(spec);

      assertThat(result).isFalse();
    }
  }

  @Nested
  class Map {

    @Test
    public void happyPath() {
      DataType key = new PrimitiveType(ProtocolConstants.DataType.VARCHAR);
      DataType value = new PrimitiveType(ProtocolConstants.DataType.INT);
      ColumnMetadata spec =
          new DefaultColumnMetadata(
              CqlIdentifier.fromInternal("keyspace"),
              CqlIdentifier.fromInternal("collection"),
              CqlIdentifier.fromInternal("column"),
              new DefaultMapType(key, value, false),
              false);

      CqlColumnMatcher.Map matcher =
          new CqlColumnMatcher.Map(
              CqlIdentifier.fromInternal("column"),
              new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
              new PrimitiveType(ProtocolConstants.DataType.INT));
      boolean result = matcher.test(spec);

      assertThat(result).isTrue();
    }

    @Test
    public void wrongValue() {
      DataType key = new PrimitiveType(ProtocolConstants.DataType.VARCHAR);
      DataType value = new PrimitiveType(ProtocolConstants.DataType.INT);
      ColumnMetadata spec =
          new DefaultColumnMetadata(
              CqlIdentifier.fromInternal("keyspace"),
              CqlIdentifier.fromInternal("collection"),
              CqlIdentifier.fromInternal("column"),
              new DefaultMapType(key, value, false),
              false);

      CqlColumnMatcher.Map matcher =
          new CqlColumnMatcher.Map(
              CqlIdentifier.fromInternal("column"),
              new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
              new PrimitiveType(ProtocolConstants.DataType.FLOAT));
      boolean result = matcher.test(spec);

      assertThat(result).isFalse();
    }

    @Test
    public void wrongKey() {
      DataType key = new PrimitiveType(ProtocolConstants.DataType.VARCHAR);
      DataType value = new PrimitiveType(ProtocolConstants.DataType.INT);
      ColumnMetadata spec =
          new DefaultColumnMetadata(
              CqlIdentifier.fromInternal("keyspace"),
              CqlIdentifier.fromInternal("collection"),
              CqlIdentifier.fromInternal("column"),
              new DefaultMapType(key, value, false),
              false);

      CqlColumnMatcher.Map matcher =
          new CqlColumnMatcher.Map(
              CqlIdentifier.fromInternal("column"),
              new PrimitiveType(ProtocolConstants.DataType.INT),
              new PrimitiveType(ProtocolConstants.DataType.INT));
      boolean result = matcher.test(spec);

      assertThat(result).isFalse();
    }

    @Test
    public void notMap() {
      ColumnMetadata spec =
          new DefaultColumnMetadata(
              CqlIdentifier.fromInternal("keyspace"),
              CqlIdentifier.fromInternal("collection"),
              CqlIdentifier.fromInternal("column"),
              new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
              false);

      CqlColumnMatcher.Map matcher =
          new CqlColumnMatcher.Map(
              CqlIdentifier.fromInternal("column"),
              new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
              new PrimitiveType(ProtocolConstants.DataType.INT));
      boolean result = matcher.test(spec);

      assertThat(result).isFalse();
    }

    @Test
    public void wrongColumn() {
      DataType key = new PrimitiveType(ProtocolConstants.DataType.VARCHAR);
      DataType value = new PrimitiveType(ProtocolConstants.DataType.INT);
      ColumnMetadata spec =
          new DefaultColumnMetadata(
              CqlIdentifier.fromInternal("keyspace"),
              CqlIdentifier.fromInternal("collection"),
              CqlIdentifier.fromInternal("column"),
              new DefaultMapType(key, value, false),
              false);

      CqlColumnMatcher.Map matcher =
          new CqlColumnMatcher.Map(
              CqlIdentifier.fromInternal("wrong"),
              new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
              new PrimitiveType(ProtocolConstants.DataType.INT));
      boolean result = matcher.test(spec);

      assertThat(result).isFalse();
    }
  }

  @Nested
  class Set {

    @Test
    public void happyPath() {
      ColumnMetadata spec =
          new DefaultColumnMetadata(
              CqlIdentifier.fromInternal("keyspace"),
              CqlIdentifier.fromInternal("collection"),
              CqlIdentifier.fromInternal("column"),
              new DefaultSetType(new PrimitiveType(ProtocolConstants.DataType.VARCHAR), false),
              false);

      CqlColumnMatcher.Set matcher =
          new CqlColumnMatcher.Set(
              CqlIdentifier.fromInternal("column"),
              new PrimitiveType(ProtocolConstants.DataType.VARCHAR));
      boolean result = matcher.test(spec);

      assertThat(result).isTrue();
    }

    @Test
    public void wrongType() {
      ColumnMetadata spec =
          new DefaultColumnMetadata(
              CqlIdentifier.fromInternal("keyspace"),
              CqlIdentifier.fromInternal("collection"),
              CqlIdentifier.fromInternal("column"),
              new DefaultSetType(new PrimitiveType(ProtocolConstants.DataType.VARCHAR), false),
              false);

      CqlColumnMatcher.Set matcher =
          new CqlColumnMatcher.Set(
              CqlIdentifier.fromInternal("column"),
              new PrimitiveType(ProtocolConstants.DataType.INT));
      boolean result = matcher.test(spec);

      assertThat(result).isFalse();
    }

    @Test
    public void notSet() {
      ColumnMetadata spec =
          new DefaultColumnMetadata(
              CqlIdentifier.fromInternal("keyspace"),
              CqlIdentifier.fromInternal("collection"),
              CqlIdentifier.fromInternal("column"),
              new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
              false);

      CqlColumnMatcher.Set matcher =
          new CqlColumnMatcher.Set(
              CqlIdentifier.fromInternal("column"),
              new PrimitiveType(ProtocolConstants.DataType.INT));
      boolean result = matcher.test(spec);

      assertThat(result).isFalse();
    }

    @Test
    public void wrongColumn() {
      ColumnMetadata spec =
          new DefaultColumnMetadata(
              CqlIdentifier.fromInternal("keyspace"),
              CqlIdentifier.fromInternal("collection"),
              CqlIdentifier.fromInternal("column"),
              new DefaultSetType(new PrimitiveType(ProtocolConstants.DataType.VARCHAR), false),
              false);

      CqlColumnMatcher.Set matcher =
          new CqlColumnMatcher.Set(
              CqlIdentifier.fromInternal("wrong"),
              new PrimitiveType(ProtocolConstants.DataType.VARCHAR));
      boolean result = matcher.test(spec);

      assertThat(result).isFalse();
    }
  }
}
