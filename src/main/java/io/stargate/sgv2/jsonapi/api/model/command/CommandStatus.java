package io.stargate.sgv2.jsonapi.api.model.command;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Enum with its json property name which is returned in api response inside status */
public enum CommandStatus {
  /** The element has the count of document */
  @JsonProperty("count")
  COUNTED_DOCUMENT,
  /** The element has the count of deleted documents */
  @JsonProperty("deletedCount")
  DELETED_COUNT,
  /**
   * Status for reporting existing namespaces. findNamespaces command is deprecated, keep this
   * jsonProperty to support backwards-compatibility. Use "namespaces" when old command name used
   * and "keyspaces" for new one.
   */
  @JsonProperty("namespaces")
  EXISTING_NAMESPACES,
  /** Status for reporting existing keyspaces. */
  @JsonProperty("keyspaces")
  EXISTING_KEYSPACES,
  /** Status for reporting existing embedding services. */
  @JsonProperty("embeddingProviders")
  EXISTING_VECTOR_PROVIDERS,
  /** Status for reporting existing collections. */
  @JsonProperty("collections")
  EXISTING_COLLECTIONS,
  /** Status for reporting existing tables. */
  @JsonProperty("tables")
  EXISTING_TABLES,
  /** Status for reporting existing indexes. */
  @JsonProperty("indexes")
  EXISTING_INDEXES,

  /**
   * List of response entries, one for each document we tried to insert with {@code insertMany}
   * command. Each entry has 2 mandatory fields: {@code _id} (document id), and {@code status} (one
   * of {@code OK}, {@code ERROR} or {@code SKIP}; {@code ERROR} entries also have {@code errorsIdx}
   * field that refers to position of the error in the root level {@code errors} List.
   */
  @JsonProperty("documentResponses")
  DOCUMENT_RESPONSES,
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
  /**
   * The element has the session id of offline writer, which is used to write the data offline to
   * the SSTable files for example
   */
  @JsonProperty("offlineWriterSessionId")
  OFFLINE_WRITER_SESSION_ID,

  /**
   * The element has the status of offline writer session, which is used to write the data offline
   * to
   */
  @JsonProperty("offlineWriterSessionStatus")
  OFFLINE_WRITER_SESSION_STATUS,
  /** The element has value 1 if collection is created */
  @JsonProperty("ok")
  OK,

  /** Next page state value that can be used in client side for pagination */
  @JsonProperty("nextPageState")
  PAGE_STATE,

  /** Sort vector value used for the ANN seatch */
  @JsonProperty("sortVector")
  SORT_VECTOR,
  /**
   * The element has the document id of newly inserted document part of update, when upserted option
   * is 'true' and no document available in DB for matching condition
   */
  @JsonProperty("upsertedId")
  UPSERTED_ID,

  /** warning value used for commandResult status */
  @JsonProperty("warnings")
  WARNINGS,

  /**
   * The element contains the schema that describes the structure of the insertedIds, only present
   * when working with Tables.
   *
   * <p>The value of <code>insertedId</code> element in the status is an array of values. For
   * collections this is an array of the value for <code>_id</code> field for the inserted
   * documents. For tables, where the primary key of the table may be multiple columns, the value of
   * the items in the array is an array of the primary key values for the inserted rows. For
   * example, with two columns in the PK it may be <code>[ [2000, "aaron"], [2001, "bob"]</code>.
   * The schema tells the client what the structure of the array is using the same layout for field
   * definitions as the is used in createTable command.
   */
  @JsonProperty("primaryKeySchema")
  PRIMARY_KEY_SCHEMA,

  /**
   * The schema of the columns that were requested in the projection.
   *
   * <p>When doing a read, the result of the read by default does not columns that have null values.
   * Additionally, in the default JSON representation multiple Column types may be represented as a
   * single JSON type, such as dates, timestamps, duration all represented as a string. Or all
   * numeric types as a JSON number.
   *
   * <p>Clients can use the schema returned here to understand where null values were omitted and
   * what the database type of the column is.
   */
  @JsonProperty("projectionSchema")
  PROJECTION_SCHEMA,

  /**
   * The count of the number of rows that were read from the database and sorted in memory.
   *
   * <p>Sorting in memory is done when a sort clause uses columns that are not from the partition
   * sort key. This type of sorting usually needs to read the entire table to sort the rows, and
   * needs to keep in memory the skip + limit number of rows. Check documentation for ways to avoid
   * in memory sorting.
   */
  @JsonProperty("sortedRowCount")
  SORTED_ROW_COUNT,
  ;
}
