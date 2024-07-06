package io.stargate.sgv2.jsonapi.service.schema.model;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultColumnMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultTableMetadata;
import com.datastax.oss.driver.internal.core.type.DefaultTupleType;
import com.datastax.oss.driver.internal.core.type.PrimitiveType;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import java.util.*;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class JsonapiTableMatcherTest {

  JsonapiTableMatcher tableMatcher = new JsonapiTableMatcher();

  @Nested
  class BuiltConditionPredicateTest {

    // NOTE: happy path asserted in the integration test

    @Test
    public void partitionColumnTypeNotMatching() {
      List<ColumnMetadata> partitionKey =
          List.of(
              new DefaultColumnMetadata(
                  CqlIdentifier.fromInternal("keyspace"),
                  CqlIdentifier.fromInternal("collection"),
                  CqlIdentifier.fromInternal("key"),
                  new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
                  false));
      TableMetadata table =
          new DefaultTableMetadata(
              CqlIdentifier.fromInternal("keyspace"),
              CqlIdentifier.fromInternal("collection"),
              UUID.randomUUID(),
              false,
              false,
              partitionKey,
              new HashMap<>(),
              new HashMap<>(),
              new HashMap<>(),
              new HashMap<>());

      boolean result = tableMatcher.test(table);

      assertThat(result).isFalse();
    }

    @Test
    public void partitionColumnsTooMany() {
      ColumnMetadata anotherPartitionColumn =
          new DefaultColumnMetadata(
              CqlIdentifier.fromInternal("keyspace"),
              CqlIdentifier.fromInternal("collection"),
              CqlIdentifier.fromInternal("key2"),
              new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
              false);

      List<ColumnMetadata> partitionColumns = createCorrectPartitionColumn();
      partitionColumns.add(anotherPartitionColumn);
      TableMetadata table =
          new DefaultTableMetadata(
              CqlIdentifier.fromInternal("keyspace"),
              CqlIdentifier.fromInternal("collection"),
              UUID.randomUUID(),
              false,
              false,
              partitionColumns,
              new HashMap<>(),
              new HashMap<>(),
              new HashMap<>(),
              new HashMap<>());

      boolean result = tableMatcher.test(table);

      assertThat(result).isFalse();
    }

    @Test
    public void clusteringColumnsCountNotMatching() {
      ColumnMetadata key =
          new DefaultColumnMetadata(
              CqlIdentifier.fromInternal("keyspace"),
              CqlIdentifier.fromInternal("collection"),
              CqlIdentifier.fromInternal("key"),
              new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
              false);
      Map<ColumnMetadata, ClusteringOrder> clusteringColumns =
          new HashMap<>() {
            {
              put(key, ClusteringOrder.ASC);
            }
          };
      TableMetadata table =
          new DefaultTableMetadata(
              CqlIdentifier.fromInternal("keyspace"),
              CqlIdentifier.fromInternal("collection"),
              UUID.randomUUID(),
              false,
              false,
              createCorrectPartitionColumn(),
              clusteringColumns,
              new HashMap<>(),
              new HashMap<>(),
              new HashMap<>());

      boolean result = tableMatcher.test(table);

      assertThat(result).isFalse();
    }

    @Test
    public void columnsCountTooLess() {
      DefaultColumnMetadata columnMetadata =
          new DefaultColumnMetadata(
              CqlIdentifier.fromInternal("keyspace"),
              CqlIdentifier.fromInternal("collection"),
              CqlIdentifier.fromInternal("key"),
              new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
              false);

      Map<CqlIdentifier, ColumnMetadata> columns = new HashMap<>();
      for (int i = 0; i < 10; i++) {
        columns.put(CqlIdentifier.fromInternal("c%s".formatted(i)), columnMetadata);
      }

      TableMetadata table =
          new DefaultTableMetadata(
              CqlIdentifier.fromInternal("keyspace"),
              CqlIdentifier.fromInternal("collection"),
              UUID.randomUUID(),
              false,
              false,
              createCorrectPartitionColumn(),
              new HashMap<>(),
              columns,
              new HashMap<>(),
              new HashMap<>());

      boolean result = tableMatcher.test(table);

      assertThat(result).isFalse();
    }

    @Test
    public void columnsCountTooMuch() {
      DefaultColumnMetadata columnMetadata =
          new DefaultColumnMetadata(
              CqlIdentifier.fromInternal("keyspace"),
              CqlIdentifier.fromInternal("collection"),
              CqlIdentifier.fromInternal("key"),
              new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
              false);
      Map<CqlIdentifier, ColumnMetadata> columns = new HashMap<>();
      for (int i = 0; i < 12; i++) {
        columns.put(CqlIdentifier.fromInternal("c%s".formatted(i)), columnMetadata);
      }

      TableMetadata table =
          new DefaultTableMetadata(
              CqlIdentifier.fromInternal("keyspace"),
              CqlIdentifier.fromInternal("collection"),
              UUID.randomUUID(),
              false,
              false,
              createCorrectPartitionColumn(),
              new HashMap<>(),
              columns,
              new HashMap<>(),
              new HashMap<>());

      boolean result = tableMatcher.test(table);

      assertThat(result).isFalse();
    }

    @Test
    public void columnsNotMatching() {
      DefaultColumnMetadata columnMetadata =
          new DefaultColumnMetadata(
              CqlIdentifier.fromInternal("keyspace"),
              CqlIdentifier.fromInternal("collection"),
              CqlIdentifier.fromInternal("key"),
              new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
              false);
      Map<CqlIdentifier, ColumnMetadata> columns = new HashMap<>();
      for (int i = 0; i < 11; i++) {
        columns.put(CqlIdentifier.fromInternal("c%s".formatted(i)), columnMetadata);
      }

      TableMetadata table =
          new DefaultTableMetadata(
              CqlIdentifier.fromInternal("keyspace"),
              CqlIdentifier.fromInternal("collection"),
              UUID.randomUUID(),
              false,
              false,
              createCorrectPartitionColumn(),
              new HashMap<>(),
              columns,
              new HashMap<>(),
              new HashMap<>());

      boolean result = tableMatcher.test(table);

      assertThat(result).isFalse();
    }

    @NotNull
    private List<ColumnMetadata> createCorrectPartitionColumn() {
      List<DataType> tuple =
          Arrays.asList(
              new PrimitiveType(ProtocolConstants.DataType.TINYINT),
              new PrimitiveType(ProtocolConstants.DataType.VARCHAR));
      List<ColumnMetadata> partitionKey = new ArrayList<>();
      partitionKey.add(
          new DefaultColumnMetadata(
              CqlIdentifier.fromInternal("keyspace"),
              CqlIdentifier.fromInternal("collection"),
              CqlIdentifier.fromInternal("key"),
              new DefaultTupleType(tuple),
              false));
      return partitionKey;
    }

    @Test
    public void nullTable() {
      boolean result = tableMatcher.test(null);

      assertThat(result).isFalse();
    }
  }
}
