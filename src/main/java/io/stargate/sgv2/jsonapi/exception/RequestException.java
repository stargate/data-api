package io.stargate.sgv2.jsonapi.exception;

/**
 * Base for any errors that are from the {@link ErrorFamily#REQUEST} family, these are errors
 * related to the structure of the request.
 *
 * <p>Scope are defined in {@link Scope} and each represents a subclass of this class.
 *
 * <p>The {@link Code} in this class is for error codes that do not have a scope.
 *
 * <p>See {@link APIException}
 */
public class RequestException extends APIException {

  public static final ErrorFamily FAMILY = ErrorFamily.REQUEST;

  public RequestException(ErrorInstance errorInstance) {
    super(errorInstance);
  }

  public enum Scope implements ErrorScope {
    /** See {@link DocumentException} */
    DOCUMENT,
    /** See {@link FilterException} */
    FILTER,
    /** See {@link ProjectionException} */
    PROJECTION,
    /** See {@link SchemaException} */
    SCHEMA,
    /** See {@link APISecurityException} */
    SECURITY,
    /** See {@link SortException} */
    SORT,
    /** See {@link UpdateException} */
    UPDATE,
    /** See {@link WarningException} */
    WARNING;

    @Override
    public String scope() {
      return name();
    }
  }

  public enum Code implements ErrorCode<RequestException> {
    COMMAND_ACCEPTS_NO_OPTIONS,
    COMMAND_FIELD_UNKNOWN,
    COMMAND_FIELD_VALUE_INVALID,
    COMMAND_UNKNOWN,

    HYBRID_FIELD_CONFLICT,
    HYBRID_FIELD_UNSUPPORTED_VALUE_TYPE,
    HYBRID_FIELD_UNKNOWN_SUBFIELDS,
    HYBRID_FIELD_UNSUPPORTED_SUBFIELD_VALUE_TYPE,

    INVALID_CREATE_COLLECTION_FIELD,
    MISSING_RERANK_QUERY_TEXT,

    REQUEST_NOT_JSON,
    REQUEST_STRUCTURE_MISMATCH,

    UNSUPPORTED_COLLECTION_COMMAND,
    UNSUPPORTED_CONTENT_TYPE,
    UNSUPPORTED_RERANKING_COMMAND,
    UNSUPPORTED_TABLE_COMMAND,

    UNSUPPORTED_UPDATE_DATA_TYPE // from ErrorCodeV1
  ;

    private final ErrorTemplate<RequestException> template;

    Code() {
      template = ErrorTemplate.load(RequestException.class, FAMILY, ErrorScope.NONE, name());
    }

    @Override
    public ErrorTemplate<RequestException> template() {
      return template;
    }
  }
}
