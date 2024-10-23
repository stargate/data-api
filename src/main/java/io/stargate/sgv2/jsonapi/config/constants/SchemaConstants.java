package io.stargate.sgv2.jsonapi.config.constants;

/** Constants for table schema in request and response */
public interface SchemaConstants {

  interface DataTypeFields {
    String TYPE = "type";
  }

  interface MetadataFieldsNames {
    String SCHEMA_TYPE = "com.datastax.data-api.schema-type";
    String SCHEMA_VERSION = "com.datastax.data-api.schema-def-version";
    String VECTORIZE_CONFIG = "com.datastax.data-api.vectorize-config";
  }

  interface MetadataFieldsValues {
    String SCHEMA_TYPE_TABLE_VALUE = "table";
    String SCHEMA_VERSION_VERSION = "1";
  }
}
