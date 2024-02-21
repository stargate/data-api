package io.stargate.sgv2.jsonapi.api.request;

public class FileWriterParams {
  private final String keyspaceName;
  private final String tableName;

  public FileWriterParams(String keyspaceName, String tableName) {
    this.keyspaceName = keyspaceName;
    this.tableName = tableName;
  }

  public String getKeyspaceName() {
    return this.keyspaceName;
  }

  public String getTableName() {
    return this.tableName;
  }
}
