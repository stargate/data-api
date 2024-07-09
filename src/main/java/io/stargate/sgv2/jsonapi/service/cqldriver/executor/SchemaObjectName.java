package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.datastax.oss.driver.shaded.guava.common.base.Strings;
import com.google.common.base.Preconditions;
import org.slf4j.MDC;

public record SchemaObjectName(String keyspace, String table) {

  // Marker object to use when the name is missing
  public static final String MISSING_NAME = "";

  public static final SchemaObjectName MISSING = new SchemaObjectName(MISSING_NAME, MISSING_NAME);

  @SuppressWarnings("StringEquality")
  public SchemaObjectName(String keyspace, String table) {
    // Check using reference equality for the missing value marker object, so we only allow empty
    // string if that object is used.
    Preconditions.checkArgument(
        (MISSING_NAME == keyspace) || !Strings.isNullOrEmpty(keyspace), "keyspace cannot be null");
    Preconditions.checkArgument(
        (MISSING_NAME == table) || !Strings.isNullOrEmpty(table), "table cannot be null");

    this.keyspace = keyspace;
    this.table = table;
  }

  public void addToMDC() {
    // NOTE: MUST stay as namespace for logging analysis
    MDC.put("namespace", keyspace);

    // NOTE: MUST stay as collection for logging analysis
    MDC.put("collection", table);
  }
}
