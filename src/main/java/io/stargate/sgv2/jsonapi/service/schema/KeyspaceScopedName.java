package io.stargate.sgv2.jsonapi.service.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import java.util.Objects;

public interface KeyspaceScopedName {

  CqlIdentifier keyspace();

  CqlIdentifier objectName();

  record DefaultKeyspaceScopedName(CqlIdentifier keyspace, CqlIdentifier objectName)
      implements KeyspaceScopedName {

    public DefaultKeyspaceScopedName {
      Objects.requireNonNull(keyspace, "keyspace must not be null");
      Objects.requireNonNull(objectName, "objectName must not be null");
    }
  }
}
