package io.stargate.sgv2.jsonapi.exception;

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
public class TestRequestException extends APIException {

  public static final ErrorFamily FAMILY = ErrorFamily.REQUEST;

  public TestRequestException(ErrorInstance errorInstance) {
    super(errorInstance);
  }

  public TestRequestException(
      ErrorFamily family, ErrorScope scope, String code, String title, String message) {
    super(family, scope, code, title, message);
  }

  public enum Scope implements ErrorScope {
    /** See {@link TestScopeException} */
    TEST_REQUEST_SCOPE,
    MISSING_CTOR,
    FAILING_CTOR;

    @Override
    public String scope() {
      return name();
    }
  }

  public enum Code implements ErrorCode<TestRequestException> {
    UNSCOPED_REQUEST_ERROR,
    NO_VARIABLES_TEMPLATE,
    HTTP_OVERRIDE;

    private final ErrorTemplate<TestRequestException> template;

    Code() {
      template = ErrorTemplate.load(TestRequestException.class, FAMILY, ErrorScope.NONE, name());
    }

    @Override
    public ErrorTemplate<TestRequestException> template() {
      return template;
    }
  }
}
