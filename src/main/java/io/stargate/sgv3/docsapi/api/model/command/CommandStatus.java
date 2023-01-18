package io.stargate.sgv3.docsapi.api.model.command;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum CommandStatus {
  @JsonProperty("deleteIds")
  DELETED_IDS,
  @JsonProperty("insertedIds")
  INSERTED_IDS,
  @JsonProperty("ok")
  OK;
}
