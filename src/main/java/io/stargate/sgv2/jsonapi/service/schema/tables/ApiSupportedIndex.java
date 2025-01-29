package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/** Abstract base for <b>supported</b> indexes in the API */
abstract class ApiSupportedIndex implements ApiIndexDef {

  protected final CqlIdentifier indexName;
  protected final CqlIdentifier targetColumn;
  protected final Map<String, String> indexOptions;
  protected final ApiIndexType indexType;

  ApiSupportedIndex(
      ApiIndexType indexType,
      CqlIdentifier indexName,
      CqlIdentifier targetColumn,
      Map<String, String> indexOptions) {

    this.indexType = Objects.requireNonNull(indexType, "indexType must not be null");
    this.indexName = Objects.requireNonNull(indexName, "indexName must not be null");
    this.targetColumn = Objects.requireNonNull(targetColumn, "targetColumn must not be null");
    this.indexOptions =
        Collections.unmodifiableMap(
            Objects.requireNonNull(indexOptions, "options must not be null"));
  }

  @Override
  public CqlIdentifier indexName() {
    return indexName;
  }

  @Override
  public CqlIdentifier targetColumn() {
    return targetColumn;
  }

  @Override
  public ApiIndexType indexType() {
    return indexType;
  }

  @Override
  public Map<String, String> indexOptions() {
    return indexOptions;
  }

  @Override
  public String toString() {
    return toString(false);
  }
}
