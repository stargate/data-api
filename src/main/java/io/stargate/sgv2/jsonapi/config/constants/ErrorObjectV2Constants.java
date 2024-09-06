package io.stargate.sgv2.jsonapi.config.constants;

public interface ErrorObjectV2Constants {

  /** Names of the fields to use in the JSON response for an ErrorObjectV2 */
  interface Fields {
    String FAMILY = "family";
    String SCOPE = "scope";
    String CODE = "errorCode";
    String TITLE = "title";
    String MESSAGE = "message";
    // Only included in debug mode, backwards compatible with old style
    String EXCEPTION_CLASS = "exceptionClass";
  }

  /** Tags used when tracking metrics */
  interface MetricTags {
    String ERROR_CODE = "errorCode";
    String EXCEPTION_CLASS = "exceptionClass";
  }

  /** Standard names for message template variables */
  interface TemplateVars {
    String SCHEMA_TYPE = "schemaType";
    String KEYSPACE = "keyspace";
    String TABLE = "table";
    String ERROR_CLASS = "errorClass";
    String ERROR_MESSAGE = "errorMessage";
  }
}
