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

  public static final ErrorFamily FAMILY = ErrorFamily.REQUEST;

  public RequestException(ErrorInstance errorInstance) {
    super(errorInstance);
  }

  public RequestException(
      ErrorFamily family, ErrorScope scope, String code, String title, String message) {
    super(family, scope, code, title, message);
  }

  public enum Scope implements ErrorScope {
    /** See {@link FilterException} */
    FILTER;

    @Override
    public String scope() {
      return name();
    }
  }

  // TODO: this is here to show how we would handle a request error that does not have a scope.
  public enum Code implements ErrorCode<RequestException> {
    FAKE_CODE;

    private final ErrorTemplate<RequestException> template;

    Code() {
      template = ErrorTemplate.load(RequestException.class, FAMILY, ErrorScope.NONE, name());
    }

    @Override
    public ErrorTemplate<RequestException> template() {
      return template;
    }
  }
}