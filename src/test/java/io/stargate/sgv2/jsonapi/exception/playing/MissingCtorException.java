package io.stargate.sgv2.jsonapi.exception.playing;

import java.util.Optional;

/**
 * An exception that is missing the CTOR we need for the templating system.
 *
 * <p>This class still has to have a store that calls the super, so it's kind of hard to actually
 * make this mistake
 *
 * <p>See {@link BadExceptionTemplateTest}
 */
public class MissingCtorException extends TestRequestException {

  public static final Scope SCOPE = Scope.MISSING_CTOR;

  // This ctor is just here to make it compile
  public MissingCtorException() {
    super(
        new ErrorInstance(
            null, ErrorFamily.REQUEST, SCOPE, "FAKE", "title", "body", Optional.empty()));
  }

  // This is the CTOR that is missing, kept commented out to show what is missing
  //  public MissingCtorException(ErrorInstance errorInstance) {
  //    super(errorInstance);
  //  }

  // there is no Code enum to avoid confusion, see the test class for why
}
