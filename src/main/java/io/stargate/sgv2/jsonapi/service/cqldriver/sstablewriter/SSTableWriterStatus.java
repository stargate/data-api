package io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter;

public record SSTableWriterStatus(String keyspace, String tableName) {
  public SSTableWriterStatus {
    if (keyspace == null || tableName == null) {
      throw new IllegalArgumentException("Keyspace and table name must not be null");
    }
  }
}
