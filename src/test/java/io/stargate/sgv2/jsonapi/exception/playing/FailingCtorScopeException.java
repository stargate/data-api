package io.stargate.sgv2.jsonapi.exception.playing;

/** An exception throws an error when you try to create it See {@link BadExceptionTemplateTest} */
public class FailingCtorScopeException extends TestRequestException {

  public static final Scope SCOPE = Scope.FAILING_CTOR;

  public FailingCtorScopeException(ErrorInstance errorInstance) {
    super(errorInstance);
    throw new RuntimeException("BANG");
  }

  public enum Code implements ErrorCode<FailingCtorScopeException> {
    EXCEPTION_FAILING_CTOR;

    private final ErrorTemplate<FailingCtorScopeException> template;

    Code() {
      template = ErrorTemplate.load(FailingCtorScopeException.class, FAMILY, SCOPE, name());
    }

    @Override
    public ErrorTemplate<FailingCtorScopeException> template() {
      return template;
    }
  }
}
