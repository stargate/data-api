package io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter;

public record OfflineWriterSessionStatus(String sessionId, String keyspace, String tableName) {
  public OfflineWriterSessionStatus {
    if (sessionId == null || keyspace == null || tableName == null) {
      throw new IllegalArgumentException("Session ID, keyspace, and table name must not be null");
    }
  }
}
