package io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter;

import java.util.List;

public record SchemaInfo(
    /* Name of the keypsace */
    String keyspaceName,
    /* Name of the table */
    String tableName,
    /* CQL to create the table */
    String createTableCQL,
    /* CQL to create the indices*/
    List<String> indexCQLs) {
  public SchemaInfo {
    if (keyspaceName == null || keyspaceName.isBlank()) {
      throw new IllegalArgumentException("keyspaceName cannot be null or empty");
    }
    if (tableName == null || tableName.isBlank()) {
      throw new IllegalArgumentException("tableName cannot be null or empty");
    }
    if (createTableCQL == null || createTableCQL.isBlank()) {
      throw new IllegalArgumentException("createTableCQL cannot be null or empty");
    }
    // when no-index option is specified by the user, indexCQLs will be null or empty
  }
}
