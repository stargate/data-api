package io.stargate.sgv2.jsonapi.exception;

public class DatabaseException extends ServerException {

  public static final Scope SCOPE = Scope.DATABASE;

  public DatabaseException(ErrorInstance errorInstance) {
    super(errorInstance);
  }

  public enum Code implements ErrorCode<DatabaseException> {
    COLLECTION_CREATION_ERROR, // converted from ErrorCodeV1
    COLLECTION_NO_INDEX_ERROR, // NO_INDEX_ERROR  // converted from ErrorCodeV1.NO_INDEX_ERROR
    COLLECTION_SCHEMA_VERSION_INVALID, // converted from ErrorCodeV1.INVALID_SCHEMA_VERSION
    COUNT_READ_FAILED, // converted from ErrorCodeV1
    DOCUMENT_FROM_DB_UNPARSEABLE,
    FAILED_CONCURRENT_OPERATIONS,
    FAILED_COMPARE_AND_SET,
    FAILED_READ_REQUEST,
    FAILED_TO_CONNECT_TO_DATABASE,
    FAILED_TRUNCATION,
    FAILED_WRITE_REQUEST,
    INVALID_DATABASE_QUERY,
    TIMEOUT_READING_DATA,
    TIMEOUT_WRITING_DATA,
    UNAUTHORIZED_ACCESS,
    UNAVAILABLE_DATABASE,
    UNEXPECTED_DOCUMENT_ID_TYPE,
    UNEXPECTED_DRIVER_ERROR,
    UNKNOWN_KEYSPACE,
    UNSUPPORTED_DATABASE_QUERY;

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
