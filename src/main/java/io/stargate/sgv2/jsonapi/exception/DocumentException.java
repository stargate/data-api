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
    DOCUMENT_ALREADY_EXISTS, // converted from ErrorCodeV1 -- in use by client DO NOT RENAME
    DOCUMENT_LEXICAL_CONTENT_TOO_BIG,
    DOCUMENT_REPLACE_DIFFERENT_DOCID,

    INVALID_COLUMN_VALUES,
    MISSING_PRIMARY_KEY_COLUMNS,

    SHRED_BAD_BINARY_VECTOR_VALUE,
    SHRED_BAD_DOCID_TYPE,
    SHRED_BAD_DOCID_VALUE,
    SHRED_BAD_DOCUMENT_TYPE,
    SHRED_BAD_DOCUMENT_VECTOR_TYPE,
    SHRED_BAD_DOCUMENT_LEXICAL_TYPE,
    SHRED_BAD_EJSON_VALUE,
    SHRED_BAD_FIELD_NAME, // from ErrorV1.SHRED_DOC_KEY_NAME_VIOLATION
    SHRED_BAD_VECTOR_SIZE,
    SHRED_BAD_VECTOR_VALUE,
    SHRED_DOC_LIMIT_VIOLATION,

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
