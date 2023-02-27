package io.stargate.sgv2.jsonapi.api.model.command;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Enum with it's json property name which is returned in api response inside status */
public enum CommandStatus {
  /** The element has the count of document */
  @JsonProperty("counted_documents")
  COUNTED_DOCUMENT,
  /** The element has the count of deleted documents */
  @JsonProperty("deletedCount")
  DELETED_COUNT,
  /** The element has the list of inserted ids */
  @JsonProperty("insertedIds")
  INSERTED_IDS,
  /** The element has the count of document read for the update operation */
  @JsonProperty("matchedCount")
  MATCHED_COUNT,

  /** The element has the count of document modified for the update operation */
  @JsonProperty("modifiedCount")
  MODIFIED_COUNT,
  /**
   * The element with boolean 'true' represents if more document to be processed for updateMany and
   * deleteMany commands
   */
  @JsonProperty("moreData")
  MORE_DATA,
  /** The element has value 1 if collection is created */
  @JsonProperty("ok")
  OK,
  /**
   * The element has the document id of newly inserted document part of update, when upserted option
   * is 'true' and no document available in DB for matching condition
   */
  @JsonProperty("upsertedId")
  UPSERTED_ID;
}
