package io.stargate.sgv2.jsonapi.api.request;

public record FileWriterParams(
    String keyspaceName,
    String tableName,
    String ssTableOutputDirectory,
    String createTableCQL,
    String insertStatementCQL) {
  public FileWriterParams {
    if (keyspaceName == null
        || tableName == null
        || ssTableOutputDirectory == null
        || createTableCQL == null
        || insertStatementCQL == null) {
      throw new IllegalArgumentException("All parameters must be non-null");
    }
  }
}
