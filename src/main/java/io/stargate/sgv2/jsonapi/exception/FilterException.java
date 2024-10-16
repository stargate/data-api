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
    NO_FILTER_UPDATE_DELETE,
    NON_PRIMARY_KEY_COLUMNS_USED_UPDATE_DELETE,
    INCOMPLETE_PRIMARY_KEY_FILTER,
    PRIMARY_KEY_NOT_FULLY_SPECIFIED;

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
