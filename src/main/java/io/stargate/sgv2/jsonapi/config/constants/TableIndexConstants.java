package io.stargate.sgv2.jsonapi.config.constants;

/** Constants for table indexes in request and response */
public interface TableIndexConstants {
  interface IndexOptionKeys {
    String ASCII_OPTION = "ascii";
    String CASE_SENSITIVE_OPTION = "case_sensitive";
    String NORMALIZE_OPTION = "normalize";
    String SOURCE_MODEL_OPTION = "source_model";
    String SIMILARITY_FUNCTION_OPTION = "similarity_function";
  }
}
