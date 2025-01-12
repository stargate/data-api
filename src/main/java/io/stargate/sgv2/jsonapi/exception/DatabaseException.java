package io.stargate.sgv2.jsonapi.exception;

public class DatabaseException extends ServerException {

  public static final Scope SCOPE = Scope.DATABASE;

  public DatabaseException(ErrorInstance errorInstance) {
    super(errorInstance);
  }

  public enum Code implements ErrorCode<DatabaseException> {
    CLOSED_CONNECTION,
    TABLE_WRITE_TIMEOUT,
    UNAVAILABLE_DATABASE,
    UNAUTHORIZED_ACCESS,
    UNEXPECTED_DRIVER_ERROR,
    UNKNOWN_KEYSPACE
    ;

    private final ErrorTemplate<DatabaseException> template;

    Code() {
      template = ErrorTemplate.load(DatabaseException.class, FAMILY, SCOPE, name());
    }

    @Override
    public ErrorTemplate<DatabaseException> template() {
      return template;
    }
  }
}
