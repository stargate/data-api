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
    MISSING_SAI_INDEX,
    FILTER_NE_AGAINST_SAI_INDEXED_COLUMN_THAT_NEED_ALLOWING_FILTERING;

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
