package io.stargate.sgv2.jsonapi.exception;

public class SchemaException extends RequestException {

  public static final Scope SCOPE = Scope.SCHEMA;

  public SchemaException(ErrorInstance errorInstance) {
    super(errorInstance);
  }

  public enum Code implements ErrorCode<SchemaException> {
    COLUMN_DEFINITION_MISSING,
    COLUMN_TYPE_UNDEFINED,
    COLUMN_TYPE_UNSUPPORTED,
    MISSING_PRIMARY_KEYS,
    PRIMARY_KEY_DEFINITION_INCORRECT;

    private final ErrorTemplate<SchemaException> template;

    Code() {
      template = ErrorTemplate.load(SchemaException.class, FAMILY, SCOPE, name());
    }

    @Override
    public ErrorTemplate<SchemaException> template() {
      return template;
    }
  }
}
