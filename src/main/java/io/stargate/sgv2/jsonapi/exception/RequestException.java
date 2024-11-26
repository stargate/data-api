package io.stargate.sgv2.jsonapi.exception;

/**
 * Base for any errors that are from the {@link ErrorFamily#REQUEST} family, these are errors
 * related to the structure of the request.
 *
 * <p>Scope are defined in {@lnk Scope} and each represents a subclass of this class.
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
    /** See {@link SchemaException} */
    SCHEMA,
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
    UNSUPPORTED_TABLE_COMMAND,
    UNSUPPORTED_COLLECTION_COMMAND,
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
