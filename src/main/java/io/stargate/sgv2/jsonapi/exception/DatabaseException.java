package io.stargate.sgv2.jsonapi.exception;

public class DatabaseException extends ServerException {

  public static final Scope SCOPE = Scope.DATABASE;

  public DatabaseException(ErrorInstance errorInstance) {
    super(errorInstance);
  }

  public enum Code implements ErrorCode<DatabaseException> {
    CLOSED_CONNECTION,
    DRIVER_TIMEOUT,
    NO_NODE_AVAILABLE,
    TABLE_WRITE_TIMEOUT;

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
