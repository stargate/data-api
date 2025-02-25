package io.stargate.sgv2.jsonapi.exception;

/**
 * Errors related to the document user is trying to insert or update, could be for collections or
 * tables
 *
 * <p>See {@link APIException} for steps to add a new code.
 */
public class DocumentException extends RequestException {

  public static final Scope SCOPE = Scope.DOCUMENT;

  public DocumentException(ErrorInstance errorInstance) {
    super(errorInstance);
  }

  public enum Code implements ErrorCode<DocumentException> {
    INVALID_COLUMN_VALUES,
    MISSING_PRIMARY_KEY_COLUMNS,
    NULL_IS_NOT_ALLOWED_FOR_MAP_SET_LIST,
    UNKNOWN_TABLE_COLUMNS,
    UNSUPPORTED_COLUMN_TYPES,
    UNSUPPORTED_VECTORIZE_CONFIGURATIONS,
    UNSUPPORTED_VECTORIZE_WHEN_MISSING_VECTORIZE_DEFINITION;

    private final ErrorTemplate<DocumentException> template;

    Code() {
      template = ErrorTemplate.load(DocumentException.class, FAMILY, SCOPE, name());
    }

    @Override
    public ErrorTemplate<DocumentException> template() {
      return template;
    }
  }
}
