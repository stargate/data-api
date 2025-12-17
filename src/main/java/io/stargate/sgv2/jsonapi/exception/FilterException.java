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
    CANNOT_LEXICAL_FILTER_NON_INDEXED_COLUMNS,
    FILTER_MULTIPLE_ID_FILTER,
    INVALID_FILTER_COLUMN_VALUES,
    INVALID_MAP_SET_LIST_FILTER,
    INVALID_PRIMARY_KEY_FILTER,
    MISSING_FILTER_FOR_UPDATE_DELETE,
    MISSING_FULL_PRIMARY_KEY_FOR_UPDATE_DELETE,
    UNKNOWN_TABLE_COLUMNS,
    UNSUPPORTED_COLUMN_TYPES,
    UNSUPPORTED_COMPARISON_FILTER_AGAINST_DURATION,
    UNSUPPORTED_FILTERING_FOR_COLUMN_TYPES,
    UNSUPPORTED_FILTER_FOR_UPDATE_ONE_DELETE_ONE,
    UNSUPPORTED_NON_PRIMARY_KEY_FILTER_FOR_UPDATE_DELETE;

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
