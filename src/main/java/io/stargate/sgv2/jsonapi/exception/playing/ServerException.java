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

  public ServerException(ErrorInstance errorInstance) {
    super(errorInstance);
  }

  public ServerException(
      ErrorFamily family, ErrorScope scope, String code, String title, String message) {
    super(family, scope, code, title, message);
  }
}
