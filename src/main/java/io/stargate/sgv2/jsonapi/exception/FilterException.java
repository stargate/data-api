package io.stargate.sgv2.jsonapi.exception;

/**
 * Errors related to the filter clause in a request.
 *
 * <p>See {@link APIException} for steps to add a new code.
 */
public class FilterException extends RequestException {

  public static final Scope SCOPE = Scope.FILTER;

  public FilterException(ErrorInstance errorInstance) {
    super(errorInstance);
  }

  public enum Code implements ErrorCode<FilterException> {
    // TODO, these Codes do not have a match in errors.yaml.
    //    MULTIPLE_ID_FILTER,
    //    FIELDS_LIMIT_VIOLATION,
    UNKNOWN_TABLE_COLUMNS;

    private final ErrorTemplate<FilterException> template;

    Code() {
      template = ErrorTemplate.load(FilterException.class, FAMILY, SCOPE, name());
    }

    @Override
    public ErrorTemplate<FilterException> template() {
      return template;
    }
  }
}
