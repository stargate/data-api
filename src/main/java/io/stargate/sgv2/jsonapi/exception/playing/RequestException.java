package io.stargate.sgv2.jsonapi.exception.playing;

/**
 * Base for any errors that are from the {@link ErrorFamily#REQUEST} family, these are errors
 * related to the structure of the request.
 *
 * <p>Scope are defined in {@lnk Scope} and each represents a subclass of this class.
 *
 * <p>The {@link Code} in this class is for error codes that do not have a scope.
 *
 * <p>See {@link APIException}
 */
public class RequestException extends APIException {

  public RequestException(ErrorInstance errorInstance) {
    super(errorInstance);
  }

  public enum Scope implements ErrorScope {
    /** See {@link FilterException} */
    FILTER;

    @Override
    public String scope() {
      return name();
    }
  }

  public enum Code implements ErrorCode<RequestException> {
    // TODO: remove fake error code, just here so it compiles
    FAKE_CODE;

    private final ErrorTemplate<RequestException> template;

    Code() {
      template =
          ErrorTemplate.load(RequestException.class, ErrorFamily.REQUEST, ErrorScope.NONE, name());
    }

    @Override
    public ErrorTemplate<RequestException> template() {
      return template;
    }
  }
}
