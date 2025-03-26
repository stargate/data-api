package io.stargate.sgv2.jsonapi.exception;

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
    /** See {@link ProviderException} */
    PROVIDER;

    @Override
    public String scope() {
      return name();
    }
  }

  public enum Code implements ErrorCode<ServerException> {
    // Error code for any unknown / unexpected server error
    UNEXPECTED_SERVER_ERROR;

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
