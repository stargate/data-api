package io.stargate.sgv2.jsonapi.util.asserts;

import static io.stargate.sgv2.jsonapi.util.ClassUtils.classSimpleName;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import java.util.function.Function;
import org.assertj.core.api.AbstractAssert;

/**
 * Base for Assert classes that are designed to assert against {@link APIException} sub-classes.
 * Most of the work is done in this class, the subclass just brings the type declarations.
 *
 * <p>For example, from {@link SchemaExceptionAssert}, the subclass sets the types, and declares a
 * factory:
 *
 * <pre>
 *   public class SchemaExceptionAssert
 *     extends APIExceptionAssert<SchemaExceptionAssert, SchemaException> {
 *
 *   private SchemaExceptionAssert(SchemaException actual) {
 *     super(actual, SchemaExceptionAssert.class);
 *   }
 *
 *   protected static SchemaExceptionAssert assertThatSchemaException(SchemaException schemaException) {
 *     return assertThatAPIException(
 *         SchemaExceptionAssert::new, SchemaException.class, schemaException);
 *   }
 *   protected static SchemaExceptionAssert assertThatSchemaException(Throwable throwable) {
 *     return assertThatAPIException(SchemaExceptionAssert::new, SchemaException.class, throwable);
 *   }
 *   ...
 * </pre>
 *
 * See {@link #assertThatAPIException(Function, Class, Throwable)}
 *
 * @param <SELF> type of the subclass, used for fluent API
 * @param <ERROR> Type of the API Exception class, e.g. {@link
 *     io.stargate.sgv2.jsonapi.exception.SchemaException}
 */
public abstract class APIExceptionAssert<
        SELF extends APIExceptionAssert<SELF, ERROR>, ERROR extends APIException>
    extends AbstractAssert<SELF, ERROR> {

  /**
   * Instantiate with values from the subclass.
   *
   * @param actual The actual error instance the assertions will run against.
   * @param selfClazz Class for {@link SELF}
   */
  protected APIExceptionAssert(ERROR actual, Class<SELF> selfClazz) {
    super(actual, selfClazz);
  }

  /**
   * Factory that subclasses can reuse for all logic to create a new instance, checks that actual
   * throwable is of the expected class.
   *
   * @param assertCtor Constructor for the subclass, called to create an instance after checking
   * @param errorClass Class for {@link ERROR} that throwable must match.
   * @param throwable Throwable we want to asset, must be instance of {@link ERROR}
   * @return New assert sub class.
   * @param <SELF> Type of subclass, same as for the class.
   * @param <ERROR> type of the APIException, same as for class.
   */
  protected static <SELF extends APIExceptionAssert<SELF, ERROR>, ERROR extends APIException>
      SELF assertThatAPIException(
          Function<ERROR, SELF> assertCtor, Class<ERROR> errorClass, Throwable throwable) {

    assertThat(throwable)
        .as("Throwable is instance of: " + classSimpleName(errorClass))
        .isInstanceOf(errorClass);

    return assertCtor.apply(errorClass.cast(throwable));
  }

  /**
   * Asserts that {@link APIException#code} matches the supplied {@link ErrorCode}
   *
   * @param expected Expected error code.
   * @return Self
   */
  public SELF hasCode(ErrorCode<?> expected) {
    isNotNull();
    assertThat(actual.code).as("APIException.code() is expected").isEqualTo(expected.name());
    return myself;
  }

  /**
   * Asserts that the {@link APIException#getMessage()} string contains each of the provided message
   * snippets.
   *
   * @param snippets vararg strings to check for
   * @return Self
   */
  public SELF hasMessageSnippets(String... snippets) {
    isNotNull();
    for (String snippet : snippets) {
      assertThat(actual.getMessage())
          .as("APIException.message() contains expected message snippet")
          .contains(snippet);
    }
    return myself;
  }
}
