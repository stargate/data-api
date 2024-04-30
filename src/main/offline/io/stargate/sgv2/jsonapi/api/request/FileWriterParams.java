package io.stargate.sgv2.jsonapi.api.request;

import java.util.List;

public record FileWriterParams(
    String keyspaceName,
    String tableName,
    String ssTableOutputDirectory,
    int fileWriterBufferSizeInMB,
    String createTableCQL,
    String insertStatementCQL,
    List<String> indexCQLs,
    Boolean vectorEnabled) {
  public FileWriterParams {
    if (keyspaceName == null || keyspaceName.isBlank()) {
      throw new IllegalArgumentException("Invalid FileWriterParams, keyspaceName is null or empty");
    }
    if (tableName == null || tableName.isBlank()) {
      throw new IllegalArgumentException("Invalid FileWriterParams, tableName is null or empty");
    }
    if (ssTableOutputDirectory == null || ssTableOutputDirectory.isBlank()) {
      throw new IllegalArgumentException(
          "Invalid FileWriterParams, ssTableOutputDirectory is null or empty");
    }
    if (fileWriterBufferSizeInMB <= 0) {
      throw new IllegalArgumentException(
          "Invalid FileWriterParams, fileWriterBufferSizeInMB is less than or equal to 0");
    }
    if (createTableCQL == null || createTableCQL.isBlank()) {
      throw new IllegalArgumentException(
          "Invalid FileWriterParams, createTableCQL is null or empty");
    }
    if (insertStatementCQL == null || insertStatementCQL.isBlank()) {
      throw new IllegalArgumentException(
          "Invalid FileWriterParams, insertStatementCQL is null or empty");
    }
    if (vectorEnabled == null) {
      throw new IllegalArgumentException("Invalid FileWriterParams, vectorEnabled is null");
    }
  }
}
