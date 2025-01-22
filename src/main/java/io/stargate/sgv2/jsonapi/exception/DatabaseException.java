package io.stargate.sgv2.jsonapi.exception;

public class DatabaseException extends ServerException {

  public static final Scope SCOPE = Scope.DATABASE;

  public DatabaseException(ErrorInstance errorInstance) {
    super(errorInstance);
  }

  public enum Code implements ErrorCode<DatabaseException> {
    FAILED_COMPARE_AND_SET,
    FAILED_TO_CONNECT_TO_DATABASE,
    FAILED_READ_REQUEST,
    FAILED_TRUNCATION,
    FAILED_WRITE_REQUEST,
    INVALID_DATABASE_QUERY,
    TIMEOUT_READING_DATA,
    TIMEOUT_WRITING_DATA,
    UNAVAILABLE_DATABASE,
    UNAUTHORIZED_ACCESS,
    UNEXPECTED_DRIVER_ERROR,
    UNKNOWN_KEYSPACE,
    UNSUPPORTED_DATABASE_QUERY
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
