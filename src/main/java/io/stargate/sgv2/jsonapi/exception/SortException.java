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
    CANNOT_LEXICAL_SORT_NON_INDEXED_COLUMNS,
    CANNOT_LEXICAL_SORT_WITH_SKIP_OPTION,
    CANNOT_SORT_ON_MULTIPLE_VECTORIZE,
    CANNOT_SORT_ON_MULTIPLE_VECTORS,
    CANNOT_SORT_ON_SPECIAL_WITH_OTHERS,
    CANNOT_SORT_UNKNOWN_COLUMNS,
    CANNOT_SORT_VECTOR_AND_NON_VECTOR_COLUMNS,
    CANNOT_VECTORIZE_SORT_NON_VECTOR_COLUMN,
    CANNOT_VECTORIZE_SORT_WHEN_MISSING_VECTORIZE_DEFINITION,
    CANNOT_VECTOR_SORT_NON_INDEXED_VECTOR_COLUMNS,
    CANNOT_VECTOR_SORT_NON_VECTOR_COLUMNS,
    CANNOT_VECTOR_SORT_WITH_LIMIT_EXCEEDS_MAX,
    CANNOT_VECTOR_SORT_WITH_SKIP_OPTION,
    INVALID_REGULAR_SORT_EXPRESSION,
    INVALID_VECTOR_SORT_EXPRESSION,
    OVERLOADED_SORT_ROW_LIMIT,
    UNSUPPORTED_PAGINATION_WITH_IN_MEMORY_SORTING,
    UNSUPPORTED_VECTOR_SORT_FOR_COLLECTION,
    UNSUPPORTED_VECTORIZE_SORT_FOR_COLLECTION,
    UNSUPPORTED_SORT_FOR_TABLE_DELETE_COMMAND,
    UNSUPPORTED_SORT_FOR_TABLE_UPDATE_COMMAND;

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
