package io.stargate.sgv2.jsonapi.api.model.command;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Enum with it's json property name which is returned in api response inside status */
public enum CommandStatus {
  /** The element has the count of document */
  @JsonProperty("counted_documents")
  COUNTED_DOCUMENT,
  /** The element has the list of deleted ids */
  @JsonProperty("deletedIds")
  DELETED_IDS,
  /** The element has the list of inserted ids */
  @JsonProperty("insertedIds")
  INSERTED_IDS,
  /** The element has value 1 if collection is created */
  @JsonProperty("ok")
  OK,
  /** The element has the list of updated ids */
  @JsonProperty("updatedIds")
  UPDATED_IDS;
}
