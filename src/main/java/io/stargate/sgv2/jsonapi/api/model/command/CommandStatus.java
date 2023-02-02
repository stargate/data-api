package io.stargate.sgv2.jsonapi.api.model.command;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum CommandStatus {
  @JsonProperty("deletedIds")
  DELETED_IDS,
  @JsonProperty("insertedIds")
  INSERTED_IDS,
  @JsonProperty("ok")
  OK,
  @JsonProperty("updatedIds")
  UPDATED_IDS;
}
