package io.stargate.sgv2.jsonapi.config.constants;

public interface TableCommentConstants {

  /** Top-level key for table comment json */
  String TOP_LEVEL_KEY = "collection";
  /** Create collection options key */
  String OPTIONS_KEY = "options";
  /** Collection name key */
  String COLLECTION_NAME_KEY = "name";
  /** Collection indexing key */
  String COLLECTION_INDEXING_KEY = "indexing";
  /** Collection vector key */
  String COLLECTION_VECTOR_KEY = "vector";
  /** Schema version key */
  String SCHEMA_VERSION_KEY = "schema_version";
  /** Default id type key */
  String DEFAULT_ID_TYPE_KEY = "default_id_type";
  /** Schema version value */
  int SCHEMA_VERSION_VALUE = 1;
}
