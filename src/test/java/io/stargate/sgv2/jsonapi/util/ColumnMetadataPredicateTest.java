package io.stargate.sgv2.jsonapi.util;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToMessageString;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultColumnMetadata;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.service.cqldriver.override.ExtendedVectorType;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Tests for the {@link ColumnMetadataPredicate}.
 *
 * <p>NOTE: previously called <code>CqlColumnMatcherTest</code>
 */
class ColumnMetadataPredicateTest {

  private final TestConstants TEST_CONSTANTS = new TestConstants();

  private final CqlIdentifier KEYSPACE = TEST_CONSTANTS.TABLE_IDENTIFIER.keyspace();
  private final CqlIdentifier TABLE = TEST_CONSTANTS.TABLE_IDENTIFIER.table();
  private final CqlIdentifier COLUMN =
      CqlIdentifier.fromInternal("column_" + TEST_CONSTANTS.CORRELATION_ID);
  private final CqlIdentifier WRONG =
      CqlIdentifier.fromInternal("wrong_" + TEST_CONSTANTS.CORRELATION_ID);

  private ColumnMetadata columnMetadata(DataType type) {
    return new DefaultColumnMetadata(KEYSPACE, TABLE, COLUMN, type, false);
  }

  @Nested
  class BasicType {

    @Test
    public void correctMatch() {
      var columnMetadata = columnMetadata(DataTypes.TEXT);
      var matcher = new ColumnMetadataPredicate.Basic(COLUMN, DataTypes.TEXT);

      assertThat(matcher.test(columnMetadata)).isTrue();
    }

    @Test
    public void wrongType() {
      var columnMetadata = columnMetadata(DataTypes.INT);
      var matcher = new ColumnMetadataPredicate.Basic(COLUMN, DataTypes.TEXT);

      assertThat(matcher.test(columnMetadata)).isFalse();
    }

    @Test
    public void notBasicType() {

      var columnMetadata = columnMetadata(DataTypes.mapOf(DataTypes.INT, DataTypes.INT, false));
      var matcher = new ColumnMetadataPredicate.Basic(COLUMN, DataTypes.TEXT);

      assertThat(matcher.test(columnMetadata)).isFalse();
    }

    @Test
    public void wrongName() {
      var columnMetadata = columnMetadata(DataTypes.TEXT);
      var matcher = new ColumnMetadataPredicate.Basic(WRONG, DataTypes.TEXT);

      assertThat(matcher.test(columnMetadata)).isFalse();
    }

    @Test
    public void toStringFormat() {
      var matcher = new ColumnMetadataPredicate.Basic(COLUMN, DataTypes.TEXT);

      assertThat(matcher.toString()).isEqualTo(cqlIdentifierToMessageString(COLUMN) + "(text)");
    }
  }

  @Nested
  class Tuple {

    @Test
    public void correctMatch() {
      var columnMetadata = columnMetadata(DataTypes.tupleOf(DataTypes.TEXT, DataTypes.INT));
      var matcher = new ColumnMetadataPredicate.Tuple(COLUMN, DataTypes.TEXT, DataTypes.INT);

      assertThat(matcher.test(columnMetadata)).isTrue();
    }

    @Test
    public void wrongOrder() {

      var columnMetadata = columnMetadata(DataTypes.tupleOf(DataTypes.TEXT, DataTypes.INT));
      var matcher = new ColumnMetadataPredicate.Tuple(COLUMN, DataTypes.INT, DataTypes.TEXT);

      assertThat(matcher.test(columnMetadata)).isFalse();
    }

    @Test
    public void wrongTuple() {
      var columnMetadata = columnMetadata(DataTypes.tupleOf(DataTypes.TEXT, DataTypes.INT));
      var matcher = new ColumnMetadataPredicate.Tuple(COLUMN, DataTypes.INT);

      assertThat(matcher.test(columnMetadata)).isFalse();
    }

    @Test
    public void notTuple() {

      var columnMetadata = columnMetadata(DataTypes.TEXT);
      var matcher = new ColumnMetadataPredicate.Tuple(COLUMN, DataTypes.INT, DataTypes.TEXT);

      assertThat(matcher.test(columnMetadata)).isFalse();
    }

    @Test
    public void wrongName() {

      var columnMetadata = columnMetadata(DataTypes.tupleOf(DataTypes.TEXT, DataTypes.INT));
      var matcher = new ColumnMetadataPredicate.Tuple(WRONG, DataTypes.TEXT, DataTypes.INT);

      assertThat(matcher.test(columnMetadata)).isFalse();
    }

    @Test
    public void toStringFormat() {
      var matcher = new ColumnMetadataPredicate.Tuple(COLUMN, DataTypes.TEXT, DataTypes.INT);

      assertThat(matcher.toString())
          .isEqualTo(cqlIdentifierToMessageString(COLUMN) + "(tuple<text, int>)");
    }
  }

  @Nested
  class Map {

    @Test
    public void correctMatch() {

      var columnMetadata = columnMetadata(DataTypes.mapOf(DataTypes.TEXT, DataTypes.INT));
      var matcher = new ColumnMetadataPredicate.Map(COLUMN, DataTypes.TEXT, DataTypes.INT);

      assertThat(matcher.test(columnMetadata)).isTrue();
    }

    @Test
    public void wrongValue() {

      var columnMetadata = columnMetadata(DataTypes.mapOf(DataTypes.TEXT, DataTypes.TINYINT));
      var matcher = new ColumnMetadataPredicate.Map(COLUMN, DataTypes.TEXT, DataTypes.INT);

      assertThat(matcher.test(columnMetadata)).isFalse();
    }

    @Test
    public void wrongKey() {

      var columnMetadata = columnMetadata(DataTypes.mapOf(DataTypes.INT, DataTypes.INT));
      var matcher = new ColumnMetadataPredicate.Map(COLUMN, DataTypes.TEXT, DataTypes.INT);

      assertThat(matcher.test(columnMetadata)).isFalse();
    }

    @Test
    public void notMap() {

      var columnMetadata = columnMetadata(DataTypes.TEXT);
      var matcher = new ColumnMetadataPredicate.Map(COLUMN, DataTypes.TEXT, DataTypes.INT);

      assertThat(matcher.test(columnMetadata)).isFalse();
    }

    @Test
    public void wrongName() {

      var columnMetadata = columnMetadata(DataTypes.mapOf(DataTypes.TEXT, DataTypes.INT));
      var matcher = new ColumnMetadataPredicate.Map(WRONG, DataTypes.TEXT, DataTypes.INT);

      assertThat(matcher.test(columnMetadata)).isFalse();
    }

    @Test
    public void toStringFormat() {
      var matcher = new ColumnMetadataPredicate.Map(COLUMN, DataTypes.TEXT, DataTypes.INT);

      assertThat(matcher.toString())
          .isEqualTo(cqlIdentifierToMessageString(COLUMN) + "(map<text, int>)");
    }
  }

  @Nested
  class Set {

    @Test
    public void correctMatch() {

      var columnMetadata = columnMetadata(DataTypes.setOf(DataTypes.TEXT));
      var matcher = new ColumnMetadataPredicate.Set(COLUMN, DataTypes.TEXT);

      assertThat(matcher.test(columnMetadata)).isTrue();
    }

    @Test
    public void wrongType() {

      var columnMetadata = columnMetadata(DataTypes.setOf(DataTypes.INT));
      var matcher = new ColumnMetadataPredicate.Set(COLUMN, DataTypes.TEXT);

      assertThat(matcher.test(columnMetadata)).isFalse();
    }

    @Test
    public void notSet() {

      var columnMetadata = columnMetadata(DataTypes.TEXT);
      var matcher = new ColumnMetadataPredicate.Set(COLUMN, DataTypes.TEXT);

      assertThat(matcher.test(columnMetadata)).isFalse();
    }

    @Test
    public void wrongName() {

      var columnMetadata = columnMetadata(DataTypes.setOf(DataTypes.TEXT));
      var matcher = new ColumnMetadataPredicate.Set(WRONG, DataTypes.TEXT);

      assertThat(matcher.test(columnMetadata)).isFalse();
    }

    @Test
    public void toStringFormat() {
      var matcher = new ColumnMetadataPredicate.Set(COLUMN, DataTypes.TEXT);

      assertThat(matcher.toString())
          .isEqualTo(cqlIdentifierToMessageString(COLUMN) + "(set<text>)");
    }
  }

  @Nested
  class Vector {

    @Test
    public void correctMatchExtendedVectorType() {

      // making sure it works for both our extended and the default type
      var columnMetadata = columnMetadata(new ExtendedVectorType(DataTypes.FLOAT, 1024));
      var matcher = new ColumnMetadataPredicate.Vector(COLUMN);

      assertThat(matcher.test(columnMetadata)).isTrue();
    }

    @Test
    public void correctMatchDefaultVectorType() {

      // making sure it works for both our extended and the default type
      var columnMetadata = columnMetadata(DataTypes.vectorOf(DataTypes.FLOAT, 1024));
      var matcher = new ColumnMetadataPredicate.Vector(COLUMN);

      assertThat(matcher.test(columnMetadata)).isTrue();
    }

    @Test
    public void wrongVectorElementType() {

      var columnMetadata = columnMetadata(DataTypes.vectorOf(DataTypes.INT, 1024));
      var matcher = new ColumnMetadataPredicate.Vector(COLUMN, DataTypes.FLOAT);

      assertThat(matcher.test(columnMetadata)).isFalse();
    }

    @Test
    public void notVector() {

      var columnMetadata = columnMetadata(DataTypes.TEXT);
      var matcher = new ColumnMetadataPredicate.Vector(COLUMN, DataTypes.FLOAT);

      assertThat(matcher.test(columnMetadata)).isFalse();
    }

    @Test
    public void wrongName() {

      var columnMetadata = columnMetadata(DataTypes.vectorOf(DataTypes.FLOAT, 1024));
      var matcher = new ColumnMetadataPredicate.Vector(WRONG, DataTypes.FLOAT);

      assertThat(matcher.test(columnMetadata)).isFalse();
    }

    @Test
    public void toStringFormat() {
      var matcher = new ColumnMetadataPredicate.Vector(COLUMN, DataTypes.FLOAT);

      assertThat(matcher.toString())
          .isEqualTo(cqlIdentifierToMessageString(COLUMN) + "(vector<float>)");
    }
  }
}
