package io.stargate.sgv2.jsonapi.service.shredding;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;

/**
 * Base interface for {@link CqlNamedValue} containers, so they can be used in a generic way,
 * regardless of order.
 */
public interface CqlNamedValueContainer
    extends NamedValueContainer<ColumnMetadata, Object, CqlNamedValue> {}
