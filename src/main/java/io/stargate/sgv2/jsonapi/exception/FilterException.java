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
    COMPARISON_FILTER_AGAINST_DURATION,
    FILTERING_NOT_SUPPORTED_FOR_TYPE,
    FILTER_REQUIRED_FOR_UPDATE_DELETE,
    FULL_PRIMARY_KEY_REQUIRED_FOR_UPDATE_DELETE,
    INCOMPLETE_PRIMARY_KEY_FILTER,
    INVALID_FILTER,
    NON_PRIMARY_KEY_FILTER_FOR_UPDATE_DELETE,
    UNKNOWN_TABLE_COLUMNS,
    UNSUPPORTED_COLUMN_TYPES,
    ;

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
