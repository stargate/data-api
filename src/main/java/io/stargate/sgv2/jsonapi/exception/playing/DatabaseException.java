package io.stargate.sgv2.jsonapi.exception.playing;

public class DatabaseException extends ServerException {
  public DatabaseException(ErrorInstance errorInstance) {
    super(errorInstance);
  }

  public enum Code implements ErrorCode<DatabaseException> {
    FAKE;

    private final ErrorTemplate<DatabaseException> template;

    Code() {
      template =
          ErrorTemplate.load(DatabaseException.class, ErrorFamily.SERVER, Scope.DATABASE, name());
    }

    @Override
    public ErrorTemplate<DatabaseException> template() {
      return template;
    }
  }
}
