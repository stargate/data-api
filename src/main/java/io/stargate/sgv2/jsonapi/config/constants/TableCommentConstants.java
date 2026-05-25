package io.stargate.sgv2.jsonapi.config.constants;

/**
 * For the current schema version see {@link
 * io.stargate.sgv2.jsonapi.service.schema.versioning.CollectionSchemaVersion#CURRENT_VERSION}
 */
public interface TableCommentConstants {

  /** Top-level key for table comment json */
  String TOP_LEVEL_KEY = "collection";

  /** Create collection options key */
  String OPTIONS_KEY = "options";

  /** Collection name key */
  String COLLECTION_NAME_KEY = "name";

  /** Collection indexing key */
  String COLLECTION_INDEXING_KEY = "indexing";

  /** Collection lexical settings configuration key */
  String COLLECTION_LEXICAL_CONFIG_KEY = "lexical";

  /** Collection rerank settings configuration key */
  String COLLECTION_RERANKING_CONFIG_KEY = "rerank";

  /** Collection vector key */
  String COLLECTION_VECTOR_KEY = "vector";

  /** Schema version key */
  String SCHEMA_VERSION_KEY = "schema_version";

  /** Default id type key */
  String DEFAULT_ID_KEY = "defaultId";
}
