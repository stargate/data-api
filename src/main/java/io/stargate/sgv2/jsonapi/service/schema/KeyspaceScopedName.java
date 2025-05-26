package io.stargate.sgv2.jsonapi.service.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;

public interface KeyspaceScopedName {

  CqlIdentifier keyspace();
  CqlIdentifier objectName();
}
