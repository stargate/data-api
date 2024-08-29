package io.stargate.sgv2.jsonapi.exception.playing;

/**
 * Errors related to the filter clause in a request.
 *
 * <p>See {@link APIException} for steps to add a new code.
 */
public class TestScopeException extends TestRequestException {

  public static final Scope SCOPE = Scope.TEST_REQUEST_SCOPE;

  public TestScopeException(ErrorInstance errorInstance) {
    super(errorInstance);
  }

  public enum Code implements ErrorCode<TestScopeException> {
    SCOPED_REQUEST_ERROR;

    private final ErrorTemplate<TestScopeException> template;

    Code() {
      template = ErrorTemplate.load(TestScopeException.class, FAMILY, SCOPE, name());
    }

    @Override
    public ErrorTemplate<TestScopeException> template() {
      return template;
    }
  }
}
