package io.stargate.sgv2.jsonapi.service.processor;

public class SSTableWriterStatus {
  private final String keyspace;
  private final String tableName;

  public SSTableWriterStatus(String keyspace, String tableName) {
    this.keyspace = keyspace;
    this.tableName = tableName;
  }

  public String getKeyspace() {
    return this.keyspace;
  }

  public String getTableName() {
    return this.tableName;
  }
}
