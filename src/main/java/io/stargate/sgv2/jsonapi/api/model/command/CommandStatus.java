package io.stargate.sgv2.jsonapi.api.model.command;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Enum with its json property name which is returned in api response inside status */
public enum CommandStatus {
  /** The element has the count of document */
  @JsonProperty(Names.COUNTED_DOCUMENT)
  COUNTED_DOCUMENT(Names.COUNTED_DOCUMENT),
  /** The element has the count of deleted documents */
  @JsonProperty(Names.DELETED_COUNT)
  DELETED_COUNT(Names.DELETED_COUNT),
  /**
   * Status for reporting existing namespaces. findNamespaces command is deprecated, keep this
   * jsonProperty to support backwards-compatibility. Use "namespaces" when old command name used
   * and "keyspaces" for new one.
   */
  @JsonProperty(Names.EXISTING_NAMESPACES)
  EXISTING_NAMESPACES(Names.EXISTING_NAMESPACES),
  /** Status for reporting existing keyspaces. */
  @JsonProperty(Names.EXISTING_KEYSPACES)
  EXISTING_KEYSPACES(Names.EXISTING_KEYSPACES),
  /** Status for reporting existing embedding services. */
  @JsonProperty(Names.EXISTING_EMBEDDING_PROVIDERS)
  EXISTING_VECTOR_PROVIDERS(Names.EXISTING_EMBEDDING_PROVIDERS),
  /** Status for reporting existing rerank services. */
  @JsonProperty(Names.EXISTING_RERANK_PROVIDERS)
  EXISTING_RERANK_PROVIDERS(Names.EXISTING_RERANK_PROVIDERS),
  /** Status for reporting existing collections. */
  @JsonProperty(Names.EXISTING_COLLECTIONS)
  EXISTING_COLLECTIONS(Names.EXISTING_COLLECTIONS),
  /** Status for reporting existing collections. */
  @JsonProperty(Names.EXISTING_TABLES)
  EXISTING_TABLES(Names.EXISTING_TABLES),
  /** Status for reporting existing indexes. */
  @JsonProperty(Names.EXISTING_INDEXES)
  EXISTING_INDEXES(Names.EXISTING_INDEXES),
  /**
   * List of response entries, one for each document we tried to insert with {@code insertMany}
   * command. Each entry has 2 mandatory fields: {@code _id} (document id), and {@code status} (one
   * of {@code OK}, {@code ERROR} or {@code SKIP}; {@code ERROR} entries also have {@code errorsIdx}
   * field that refers to position of the error in the root level {@code errors} List.
   */
  @JsonProperty(Names.DOCUMENT_RESPONSES)
  DOCUMENT_RESPONSES(Names.DOCUMENT_RESPONSES),
  /** The element has the list of inserted ids */
  @JsonProperty(Names.INSERTED_IDS)
  INSERTED_IDS(Names.INSERTED_IDS),
  /** The element has the count of document read for the update operation */
  @JsonProperty(Names.MATCHED_COUNT)
  MATCHED_COUNT(Names.MATCHED_COUNT),

  /** The element has the count of document modified for the update operation */
  @JsonProperty(Names.MODIFIED_COUNT)
  MODIFIED_COUNT(Names.MODIFIED_COUNT),
  /**
   * The element with boolean 'true' represents if more document to be processed for updateMany and
   * deleteMany commands
   */
  @JsonProperty(Names.MORE_DATA)
  MORE_DATA(Names.MORE_DATA),
  /**
   * The element has the session id of offline writer, which is used to write the data offline to
   * the SSTable files for example
   */
  @JsonProperty(Names.OFFLINE_WRITER_SESSION_ID)
  OFFLINE_WRITER_SESSION_ID(Names.OFFLINE_WRITER_SESSION_ID),

  /**
   * The element has the status of offline writer session, which is used to write the data offline
   * to
   */
  @JsonProperty(Names.OFFLINE_WRITER_SESSION_STATUS)
  OFFLINE_WRITER_SESSION_STATUS(Names.OFFLINE_WRITER_SESSION_STATUS),
  /** The element has value 1 if collection is created */
  @JsonProperty(Names.OK)
  OK(Names.OK),

  /** Next page state value that can be used in client side for pagination */
  @JsonProperty(Names.PAGE_STATE)
  PAGE_STATE(Names.PAGE_STATE),

  /** Sort vector value used for the ANN search */
  @JsonProperty(Names.SORT_VECTOR)
  SORT_VECTOR(Names.SORT_VECTOR),
  /**
   * The element has the document id of newly inserted document part of update, when upserted option
   * is 'true' and no document available in DB for matching condition
   */
  @JsonProperty(Names.UPSERTED_ID)
  UPSERTED_ID(Names.UPSERTED_ID),

  /** warning value used for commandResult status */
  @JsonProperty(Names.WARNINGS)
  WARNINGS(Names.WARNINGS),

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
  @JsonProperty(Names.PRIMARY_KEY_SCHEMA)
  PRIMARY_KEY_SCHEMA(Names.PRIMARY_KEY_SCHEMA),

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
  @JsonProperty(Names.PROJECTION_SCHEMA)
  PROJECTION_SCHEMA(Names.PROJECTION_SCHEMA),

  /**
   * The count of the number of rows that were read from the database and sorted in memory.
   *
   * <p>Sorting in memory is done when a sort clause uses columns that are not from the partition
   * sort key. This type of sorting usually needs to read the entire table to sort the rows, and
   * needs to keep in memory the skip + limit number of rows. Check documentation for ways to avoid
   * in memory sorting.
   */
  @JsonProperty(Names.SORTED_ROW_COUNT)
  SORTED_ROW_COUNT(Names.SORTED_ROW_COUNT),
  ;

  private final String apiName;

  CommandStatus(String apiName) {
    this.apiName = apiName;
  }

  public String apiName() {
    return apiName;
  }

  /**
   * Actual String constants used in the JSON response for {@link CommandStatus} elements. Have to
   * be defined here as String Constants to be used in the {@link JsonProperty} annotation and other
   * direct references.
   */
  interface Names {
    String COUNTED_DOCUMENT = "count";
    String DELETED_COUNT = "deletedCount";
    String EXISTING_NAMESPACES = "namespaces";
    String EXISTING_KEYSPACES = "keyspaces";
    String EXISTING_EMBEDDING_PROVIDERS = "embeddingProviders";
    String EXISTING_RERANK_PROVIDERS = "rerankProviders";
    String EXISTING_COLLECTIONS = "collections";
    String EXISTING_TABLES = "tables";
    String EXISTING_INDEXES = "indexes";
    String DOCUMENT_RESPONSES = "documentResponses";
    String INSERTED_IDS = "insertedIds";
    String MATCHED_COUNT = "matchedCount";
    String MODIFIED_COUNT = "modifiedCount";
    String MORE_DATA = "moreData";
    String OFFLINE_WRITER_SESSION_ID = "offlineWriterSessionId";
    String OFFLINE_WRITER_SESSION_STATUS = "offlineWriterSessionStatus";
    String OK = "ok";
    String PAGE_STATE = "nextPageState";
    String SORT_VECTOR = "sortVector";
    String UPSERTED_ID = "upsertedId";
    String WARNINGS = "warnings";
    String PRIMARY_KEY_SCHEMA = "primaryKeySchema";
    String PROJECTION_SCHEMA = "projectionSchema";
    String SORTED_ROW_COUNT = "sortedRowCount";
  }
}
