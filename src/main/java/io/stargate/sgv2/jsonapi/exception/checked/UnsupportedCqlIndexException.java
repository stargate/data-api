package io.stargate.sgv2.jsonapi.exception.checked;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToMessageString;

import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;

/** */
public class UnsupportedCqlIndexException extends CheckedApiException {

  private final IndexMetadata indexMetadata;

  public UnsupportedCqlIndexException(String reason, IndexMetadata indexMetadata) {
    this(reason, indexMetadata, null);
  }

  public UnsupportedCqlIndexException(String reason, IndexMetadata indexMetadata, Throwable cause) {
    super(
        "Unsupported CQL index definition for indexName: %s due to: "
            .formatted(reason, cqlIdentifierToMessageString(indexMetadata.getName())),
        cause);
    this.indexMetadata = indexMetadata;
  }
}
