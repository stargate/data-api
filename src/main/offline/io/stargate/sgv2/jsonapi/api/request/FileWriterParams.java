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
    if (keyspaceName == null
        || keyspaceName.isBlank()
        || tableName == null
        || tableName.isBlank()
        || ssTableOutputDirectory == null
        || ssTableOutputDirectory.isBlank()
        || fileWriterBufferSizeInMB <= 0
        || createTableCQL == null
        || createTableCQL.isBlank()
        || insertStatementCQL == null
        || insertStatementCQL.isBlank()
        || indexCQLs == null
        || indexCQLs.isEmpty()
        || vectorEnabled == null) {
      throw new IllegalArgumentException("Invalid FileWriterParams");
    }
  }
}
