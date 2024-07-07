package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.datastax.oss.driver.shaded.guava.common.base.Strings;
import com.google.common.base.Preconditions;

public record SchemaObjectName(
    String keyspace,
    String name)
{

  // check fields are not null
  public SchemaObjectName {
    if (Strings.isNullOrEmpty(keyspace)) {
      throw new IllegalArgumentException("keyspace cannot be null");
    }
    if (Strings.isNullOrEmpty(name)) {
      throw new IllegalArgumentException("name cannot be null");
    }
  }
}
