package io.stargate.sgv3.docsapi.api.model.command;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum CommandStatus {
  @JsonProperty("insertedIds")
  INSERTED_IDS,
  @JsonProperty("ok")
  CREATE_COLLECTION;
}
