package io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter;

import static org.junit.Assert.assertThrows;

import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class SchemaInfoTest {
  public static Stream<Arguments> provideInvalidSchemaInfo() {
    return Stream.of(
        Arguments.of(
            null, "table1", "CREATE TABLE table1 (id UUID PRIMARY KEY, name TEXT)", List.of()),
        Arguments.of(
            "", "table1", "CREATE TABLE table1 (id UUID PRIMARY KEY, name TEXT)", List.of()),
        Arguments.of(
            " ", "table1", "CREATE TABLE table1 (id UUID PRIMARY KEY, name TEXT)", List.of()),
        Arguments.of(
            "keyspace1", null, "CREATE TABLE table1 (id UUID PRIMARY KEY, name TEXT)", List.of()),
        Arguments.of(
            "keyspace1", "", "CREATE TABLE table1 (id UUID PRIMARY KEY, name TEXT)", List.of()),
        Arguments.of(
            "keyspace1", " ", "CREATE TABLE table1 (id UUID PRIMARY KEY, name TEXT)", List.of()),
        Arguments.of("keyspace1", "table1", null, List.of()),
        Arguments.of("keyspace1", "table1", "", List.of()),
        Arguments.of("keyspace1", "table1", " ", List.of()));
  }

  @ParameterizedTest
  @MethodSource("provideInvalidSchemaInfo")
  public void testInvalidSchemaInfo(
      String keyspaceName, String tableName, String createTableCQL, List<String> indexCQLs) {
    assertThrows(
        IllegalArgumentException.class,
        () -> new SchemaInfo(keyspaceName, tableName, createTableCQL, indexCQLs));
  }
}
