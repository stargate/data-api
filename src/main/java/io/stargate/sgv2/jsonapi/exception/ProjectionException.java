package io.stargate.sgv2.jsonapi.exception;

/**
 * Errors related to the projection clause in a request.
 *
 * <p>See {@link APIException} for steps to add a new code.
 */
public class ProjectionException extends RequestException {

  public static final Scope SCOPE = Scope.PROJECTION;

  public ProjectionException(ErrorInstance errorInstance) {
    super(errorInstance);
  }

  public enum Code implements ErrorCode<ProjectionException> {
    UNSUPPORTED_COLUMN_TYPES,

    UNSUPPORTED_PROJECTION_DEFINITION, // from ErrorCodeV1
    UNSUPPORTED_PROJECTION_PARAM, // from ErrorCodeV1
    UNKNOWN_TABLE_COLUMNS;

    private final ErrorTemplate<ProjectionException> template;

    Code() {
      template = ErrorTemplate.load(ProjectionException.class, FAMILY, SCOPE, name());
    }

    @Override
    public ErrorTemplate<ProjectionException> template() {
      return template;
    }
  }
}
