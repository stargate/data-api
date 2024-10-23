package io.stargate.sgv2.jsonapi.exception;

/**
 * Warning as RequestException for collections or tables.
 *
 * <p>See {@link APIException} for steps to add a new code.
 */
public class WarningException extends RequestException {

  public static final Scope SCOPE = Scope.WARNING;

  public WarningException(ErrorInstance errorInstance) {
    super(errorInstance);
  }

  public enum Code implements ErrorCode<WarningException> {
    COMPARISON_FILTER_UNSUPPORTED_BY_INDEXING,
    DEPRECATED_COMMAND,
    INCOMPLETE_PRIMARY_KEY_FILTER,
    MISSING_INDEX,
    NOT_EQUALS_UNSUPPORTED_BY_INDEXING,
    QUERY_RETRIED_DUE_TO_INDEXING,
    ZERO_FILTER_OPERATIONS;

    private final ErrorTemplate<WarningException> template;

    Code() {
      template = ErrorTemplate.load(WarningException.class, FAMILY, SCOPE, name());
    }

    @Override
    public ErrorTemplate<WarningException> template() {
      return template;
    }
  }
}
