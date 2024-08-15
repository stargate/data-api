package io.stargate.sgv2.jsonapi.exception.playing;

/**
 * Base for any errors that are from the {@link ErrorFamily#SERVER} family, these are server side
 * errors not related to the structure of the request itself.
 *
 * <p>Scope are defined in {@link Scope} and each represents a subclass of this class.
 *
 * <p>See {@link APIException}
 */
public class ServerException extends APIException {

  public static final ErrorFamily FAMILY = ErrorFamily.SERVER;

  public ServerException(ErrorInstance errorInstance) {
    super(errorInstance);
  }

  public enum Scope implements ErrorScope {
    /** See {@link DatabaseException} */
    DATABASE,
    /** See {@link EmbeddingProviderException} */
    EMBEDDING_PROVIDER;

    @Override
    public String scope() {
      return name();
    }
  }

  public enum Code implements ErrorCode<ServerException> {
    // TODO: remove fake error code, just here so it compiles
    FAKE_CODE;

    private final ErrorTemplate<ServerException> template;

    Code() {
      template = ErrorTemplate.load(ServerException.class, FAMILY, ErrorScope.NONE, name());
    }

    @Override
    public ErrorTemplate<ServerException> template() {
      return template;
    }
  }
}
