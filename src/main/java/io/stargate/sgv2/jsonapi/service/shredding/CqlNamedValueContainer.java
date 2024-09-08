package io.stargate.sgv2.jsonapi.service.shredding;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;

public interface CqlNamedValueContainer extends NamedValueContainer<ColumnMetadata, Object, CqlNamedValue> {
}
