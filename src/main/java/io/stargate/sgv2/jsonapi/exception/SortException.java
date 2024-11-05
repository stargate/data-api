package io.stargate.sgv2.jsonapi.exception;

/**
 * Errors relating to the sort clause, this includes sorting with ANN
 *
 * <p>See {@link APIException} for steps to add a new code.
 */
public class SortException extends RequestException {

  public static final Scope SCOPE = Scope.SORT;

  public SortException(ErrorInstance errorInstance) {
    super(errorInstance);
  }

  public enum Code implements ErrorCode<SortException> {
    CANNOT_MIX_VECTOR_AND_NON_VECTOR_SORT,
    CANNOT_SORT_TABLE_DELETE_COMMAND,
    CANNOT_SORT_TABLE_UPDATE_COMMAND,
    CANNOT_SORT_UNKNOWN_COLUMNS,
    CANNOT_VECTOR_SORT_NON_INDEXED_VECTOR_COLUMNS,
    CANNOT_VECTOR_SORT_NON_VECTOR_COLUMNS,
    CANNOT_VECTOR_SORT_WITH_SKIP_OPTION,
    MORE_THAN_ONE_VECTOR_SORT,
    CANNOT_SORT_TOO_MUCH_DATA;

    private final ErrorTemplate<SortException> template;

    Code() {
      template = ErrorTemplate.load(SortException.class, FAMILY, SCOPE, name());
    }

    @Override
    public ErrorTemplate<SortException> template() {
      return template;
    }
  }
}
