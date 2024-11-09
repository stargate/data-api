package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * An index that is not supported by the api, could be either from a user description or from CQL of
 * an existing index, represented by subtypes.
 */
public abstract class UnsupportedIndex implements ApiIndexDef {

  private final CqlIdentifier indexName;
  private final Map<String, String> options;

  UnsupportedIndex(CqlIdentifier indexName, Map<String, String> options) {
    this.indexName = Objects.requireNonNull(indexName, "indexName must not be null");
    this.options =
        Collections.unmodifiableMap(Objects.requireNonNull(options, "options must not be null"));
  }

  @Override
  public CqlIdentifier indexName() {
    return indexName;
  }

  @Override
  public CqlIdentifier targetColumn() {
    // we may not have been able to extract this from the index, e.g. this has an unsupported
    // function or something else
    throw new UnsupportedOperationException("Unsupported index does not have a target column");
  }

  @Override
  public ApiIndexType indexType() {
    throw new UnsupportedOperationException("Unsupported index does not have indexType");
  }

  @Override
  public boolean isUnsupported() {
    return true;
  }
}
