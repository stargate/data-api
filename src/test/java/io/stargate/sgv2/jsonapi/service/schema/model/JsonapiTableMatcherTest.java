package io.stargate.sgv2.jsonapi.service.schema.model;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultColumnMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultTableMetadata;
import com.datastax.oss.driver.internal.core.type.PrimitiveType;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.Schema;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@Disabled
class JsonapiTableMatcherTest {

  JsonapiTableMatcher tableMatcher = new JsonapiTableMatcher();

  @Nested
  class PredicateTest {

    // NOTE: happy path asserted in the integration test

    @Test
    public void partitionColumnTypeNotMatching() {
      List<ColumnMetadata> partitionKey =
          List.of(
              new DefaultColumnMetadata(
                  CqlIdentifier.fromCql("keyspace"),
                  CqlIdentifier.fromCql("collection"),
                  CqlIdentifier.fromCql("key"),
                  new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
                  false));
      TableMetadata table =
          new DefaultTableMetadata(
              CqlIdentifier.fromCql("keyspace"),
              CqlIdentifier.fromCql("collection"),
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
      Schema.CqlTable table =
          withCorrectPartitionColumns()
              .addPartitionKeyColumns(
                  QueryOuterClass.ColumnSpec.newBuilder().setName("key2").build())
              .build();

      boolean result = tableMatcher.test(null);

      assertThat(result).isFalse();
    }

    @Test
    public void clusteringColumnsCountNotMatching() {
      Schema.CqlTable table =
          withCorrectPartitionColumns()
              .addClusteringKeyColumns(
                  QueryOuterClass.ColumnSpec.newBuilder().setName("cluster").build())
              .build();

      boolean result = tableMatcher.test(null);

      assertThat(result).isFalse();
    }

    @Test
    public void columnsCountTooLess() {
      Schema.CqlTable.Builder tableBuilder = withCorrectPartitionColumns();
      for (int i = 0; i < 10; i++) {
        tableBuilder.addColumns(
            QueryOuterClass.ColumnSpec.newBuilder().setName("c%s".formatted(i)).build());
      }
      Schema.CqlTable table = tableBuilder.build();

      boolean result = tableMatcher.test(null);

      assertThat(result).isFalse();
    }

    @Test
    public void columnsCountTooMuch() {
      Schema.CqlTable.Builder tableBuilder = withCorrectPartitionColumns();
      for (int i = 0; i < 12; i++) {
        tableBuilder.addColumns(
            QueryOuterClass.ColumnSpec.newBuilder().setName("c%s".formatted(i)).build());
      }
      Schema.CqlTable table = tableBuilder.build();

      boolean result = tableMatcher.test(null);

      assertThat(result).isFalse();
    }

    @Test
    public void columnsNotMatching() {
      Schema.CqlTable.Builder tableBuilder = withCorrectPartitionColumns();
      for (int i = 0; i < 11; i++) {
        tableBuilder.addColumns(
            QueryOuterClass.ColumnSpec.newBuilder().setName("c%s".formatted(i)).build());
      }
      Schema.CqlTable table = tableBuilder.build();

      boolean result = tableMatcher.test(null);

      assertThat(result).isFalse();
    }

    @NotNull
    private Schema.CqlTable.Builder withCorrectPartitionColumns() {
      return Schema.CqlTable.newBuilder()
          .addPartitionKeyColumns(
              QueryOuterClass.ColumnSpec.newBuilder()
                  .setName("key")
                  .setType(
                      QueryOuterClass.TypeSpec.newBuilder()
                          .setTuple(
                              QueryOuterClass.TypeSpec.Tuple.newBuilder()
                                  .addElements(
                                      QueryOuterClass.TypeSpec.newBuilder()
                                          .setBasic(QueryOuterClass.TypeSpec.Basic.TINYINT))
                                  .addElements(
                                      QueryOuterClass.TypeSpec.newBuilder()
                                          .setBasic(QueryOuterClass.TypeSpec.Basic.VARCHAR)
                                          .build())
                                  .build()))
                  .build());
    }

    @Test
    public void nullTable() {
      boolean result = tableMatcher.test(null);

      assertThat(result).isFalse();
    }
  }
}
