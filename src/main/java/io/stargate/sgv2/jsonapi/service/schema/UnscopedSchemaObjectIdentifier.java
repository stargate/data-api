package io.stargate.sgv2.jsonapi.service.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.api.request.UserAgent;

import java.util.Objects;

/**
  * An identifier for a schema object that is not scoped to a
 * {@link io.stargate.sgv2.jsonapi.api.request.tenant.Tenant}.
 * <p>
 * We only have the Keyspace and object name, such as the table name, we do now know who it belongs to.
 * This is used by the SchemaObjectCache due to issues with not knowing if a CQL table is a
 * API Collection or API Table. See {@link SchemaObjectCache#getTableBased(RequestContext, UnscopedSchemaObjectIdentifier, UserAgent, boolean)}
 */
public interface UnscopedSchemaObjectIdentifier {

  /**
   * The keyspace that this object belongs to.
   */
  CqlIdentifier keyspace();

  /**
   * The name of the object, such as a table or index.
   */
  CqlIdentifier objectName();

  record DefaultKeyspaceScopedName(CqlIdentifier keyspace, CqlIdentifier objectName)
      implements UnscopedSchemaObjectIdentifier {

    public DefaultKeyspaceScopedName {
      Objects.requireNonNull(keyspace, "keyspace must not be null");
      Objects.requireNonNull(objectName, "objectName must not be null");
    }
  }
}
