package io.stargate.sgv2.jsonapi.config.constants;

public interface ErrorObjectV2Constants {

  /**
   * Names of the fields to use in the JSON response for an {@link
   * io.stargate.sgv2.jsonapi.api.model.command.CommandErrorV2}
   */
  interface Fields {
    // Only included in debug mode, backwards compatible with old style
    String CODE = "errorCode";
    String DOCUMENT_IDS = "documentIds";
    String EXCEPTION_CLASS = "exceptionClass";
    String FAMILY = "family";
    String ID = "id";
    String MESSAGE = "message";
    String SCOPE = "scope";
    String TITLE = "title";
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
