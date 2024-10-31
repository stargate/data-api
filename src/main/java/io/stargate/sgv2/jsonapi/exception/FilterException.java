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
    UNKNOWN_TABLE_COLUMNS,
    INVALID_FILTER,
    UNSUPPORTED_COLUMN_TYPES,
    COMPARISON_FILTER_AGAINST_DURATION,
    FILTER_REQUIRED_FOR_UPDATE_DELETE,
    FILTER_ON_COMPLEX_COLUMNS,
    NON_PRIMARY_KEY_FILTER_FOR_UPDATE_DELETE,
    INCOMPLETE_PRIMARY_KEY_FILTER,
    FULL_PRIMARY_KEY_REQUIRED_FOR_UPDATE_DELETE;

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
