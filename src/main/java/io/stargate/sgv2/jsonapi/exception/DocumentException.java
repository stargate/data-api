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
    DOCUMENT_ALREADY_EXISTS,
    // Internal error: does it belong here?
    DOCUMENT_FROM_DB_UNPARSEABLE,
    DOCUMENT_REPLACE_DIFFERENT_DOCID,

    LEXICAL_CONTENT_TOO_LONG,

    INVALID_COLUMN_VALUES,
    INVALID_VECTOR_LENGTH, // copy from V1 VECTOR_SIZE_MISMATCH("Length of vector parameter
    // different from declared '$vector' dimension"),
    MISSING_PRIMARY_KEY_COLUMNS,
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
