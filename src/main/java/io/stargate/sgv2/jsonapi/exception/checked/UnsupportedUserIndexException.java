package io.stargate.sgv2.jsonapi.exception.checked;

import io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes.IndexDefinitionDesc;

/** Thrown when the user has described an index what we cannot support. */
public class UnsupportedUserIndexException extends CheckedApiException {

  private final IndexDefinitionDesc indexDesc;

  public UnsupportedUserIndexException(String reason, IndexDefinitionDesc indexDesc) {
    this(reason, indexDesc, null);
  }

  public UnsupportedUserIndexException(
      String reason, IndexDefinitionDesc indexDesc, Throwable cause) {
    super(
        "Unsupported index description, reason: %s indexDesc: %s ".formatted(reason, indexDesc),
        cause);
    this.indexDesc = indexDesc;
  }
}
