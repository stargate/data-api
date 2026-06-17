package io.stargate.sgv2.jsonapi.util.asserts;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import java.util.function.Function;
import org.assertj.core.api.AbstractAssert;

public abstract class APIExceptionAssert<
        SELF extends APIExceptionAssert<SELF, ERROR>, ERROR extends APIException>
    extends AbstractAssert<SELF, ERROR> {

  protected APIExceptionAssert(ERROR actual, Class<SELF> selfClazz) {
    super(actual, selfClazz);
  }

  protected static <SELF extends APIExceptionAssert<SELF, ERROR>, ERROR extends APIException>
      SELF assertThatAPIException(
          Function<ERROR, SELF> assertCtor, Class<ERROR> errorClass, Throwable throwable) {

    assertThat(throwable)
        .as("Throwable is instance of " + errorClass.getSimpleName() + "")
        .isInstanceOf(errorClass);
    return assertCtor.apply(errorClass.cast(throwable));
  }

  public SELF hasCode(ErrorCode<?> expected) {
    isNotNull();
    assertThat(actual.code).isEqualTo(expected.name());
    return myself;
  }

  public SELF hasMessageSnippets(String... snippets) {
    isNotNull();
    for (String snippet : snippets) {
      assertThat(actual.getMessage()).contains(snippet);
    }
    return myself;
  }
}
