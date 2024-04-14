package io.stargate.sgv2.jsonapi.api.request;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class FileWriterParamsTest {

  @ParameterizedTest
  @MethodSource("sampleFileWriterParamsProvider")
  @DisplayName("Should expect all values in FileParams")
  public void expectAllParams(
      String keyspaceName,
      String tableName,
      String ssTableOutputDirectory,
      int fileWriterBufferSizeInMB,
      String createTableCQL,
      String insertStatementCQL,
      List<String> indexCQLs,
      Boolean vectorEnabled) {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new FileWriterParams(
                keyspaceName,
                tableName,
                ssTableOutputDirectory,
                fileWriterBufferSizeInMB,
                createTableCQL,
                insertStatementCQL,
                indexCQLs,
                vectorEnabled));
  }

  public static Arguments[] sampleFileWriterParamsProvider() {
    return new Arguments[] {
      arguments(
          null,
          "tableName",
          "ssTableOutputDirectory",
          1,
          "createTableCQL",
          "insertStatementCQL",
          List.of("indexCQLs"),
          true),
      arguments(
          "",
          "tableName",
          "ssTableOutputDirectory",
          1,
          "createTableCQL",
          "insertStatementCQL",
          List.of("indexCQLs"),
          true),
      arguments(
          "keyspaceName",
          null,
          "ssTableOutputDirectory",
          1,
          "createTableCQL",
          "insertStatementCQL",
          List.of("indexCQLs"),
          true),
      arguments(
          "keyspaceName",
          "",
          "ssTableOutputDirectory",
          1,
          "createTableCQL",
          "insertStatementCQL",
          List.of("indexCQLs"),
          true),
      arguments(
          "keyspaceName",
          "tableName",
          null,
          1,
          "createTableCQL",
          "insertStatementCQL",
          List.of("indexCQLs"),
          true),
      arguments(
          "keyspaceName",
          "tableName",
          "",
          1,
          "createTableCQL",
          "insertStatementCQL",
          List.of("indexCQLs"),
          true),
      arguments(
          "keyspaceName",
          "tableName",
          "ssTableOutputDirectory",
          0,
          "createTableCQL",
          "insertStatementCQL",
          List.of("indexCQLs"),
          true),
      arguments(
          "keyspaceName",
          "tableName",
          "ssTableOutputDirectory",
          1,
          null,
          "insertStatementCQL",
          List.of("indexCQLs"),
          true),
      arguments(
          "keyspaceName",
          "tableName",
          "ssTableOutputDirectory",
          1,
          "",
          "insertStatementCQL",
          List.of("indexCQLs"),
          true),
      arguments(
          "keyspaceName",
          "tableName",
          "ssTableOutputDirectory",
          1,
          "createTableCQL",
          null,
          List.of("indexCQLs"),
          true),
      arguments(
          "keyspaceName",
          "tableName",
          "ssTableOutputDirectory",
          1,
          "createTableCQL",
          "",
          List.of("indexCQLs"),
          true),
      arguments(
          "keyspaceName",
          "tableName",
          "ssTableOutputDirectory",
          1,
          "createTableCQL",
          "insertStatementCQL",
          null,
          true),
      arguments(
          "keyspaceName",
          "tableName",
          "ssTableOutputDirectory",
          1,
          "createTableCQL",
          "insertStatementCQL",
          List.of(),
          true),
      arguments(
          "keyspaceName",
          "tableName",
          "ssTableOutputDirectory",
          1,
          "createTableCQL",
          "insertStatementCQL",
          List.of("indexCQLs"),
          null),
    };
  }
}
