package io.stargate.sgv2.jsonapi.exception.playing;

import com.google.common.base.CharMatcher;
import com.google.common.base.Strings;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.text.StringSubstitutor;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * A template for creating an {@link APIException}, that is associated with an Error Code enum so
 * the {@link io.stargate.sgv2.jsonapi.exception.ErrorCode} interface can easily create the
 * exception.
 *
 * <p>Instances are normally created by reading a config file, see {@link #load(Class, ErrorFamily,
 * ErrorScope, String)}
 *
 * <p>Example usage with an Error Code ENUM:
 *
 * <p>
 *
 * <pre>
 *     public enum Code implements ErrorCode<FilterException> {
 *
 *     MULTIPLE_ID_FILTER,
 *     FILTER_FIELDS_LIMIT_VIOLATION;
 *
 *     private final ErrorTemplate template;
 *
 *     Code() {
 *       template = ErrorTemplate.load(FilterException.class, FAMILY, SCOPE, name());
 *     }
 *
 *     @Override
 *     public ErrorTemplate template() {
 *       return template;
 *     }
 *   }
 * </pre>
 *
 * @param constructor A constructor accepts a single parameter of {@link ErrorInstance} and returns
 *     an instance of the `T` type.
 * @param family {@link ErrorFamily} the error belongs to.
 * @param scope {@link ErrorScope} the error belongs to.
 * @param code SNAKE_CASE error code for the error.
 * @param title Title of the error, does not change between instances.
 * @param messageTemplate A template for the error message, with variables to be replaced at runtime
 *     using the {@link StringSubstitutor} from Apache Commons Text.
 * @param <T> The type of the {@link APIException} the template creates.
 */
public record ErrorTemplate<T extends APIException>(
    Constructor<T> constructor,
    ErrorFamily family,
    ErrorScope scope,
    String code,
    String title,
    String messageTemplate) {

  // TIDY: make this configurable
  private static final String DEFAULT_ERROR_FILE = "errors.yaml";

  public T toException(Map<String, String> values) {
    var errorInstance = toInstance(values);

    try {
      return constructor().newInstance(errorInstance);
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(
          "Failed to create a new instance of " + constructor().getDeclaringClass().getSimpleName(),
          e);
    }
  }

  /**
   * Uses the template to create a {@link ErrorInstance} with the provided values.
   *
   * <p>As well as the values provided in the params, the template can use "snippets" that are
   * defined in the {@link #DEFAULT_ERROR_FILE} file. See the file for infor on how to define them.
   *
   * <p>
   *
   * @param values The values to use in the template for the error message.
   * @return {@link ErrorInstance} created from the template.
   */
  private ErrorInstance toInstance(Map<String, String> values) {

    // TODO: here we would use the apache string substitution to replace the variables in the
    // messageTemplate
    // TODO; make sure we include any snippets from the config file.

    // use the apache string substitution to replace the variables in the messageTemplate
    StringSubstitutor sub = new StringSubstitutor(values);
    String msg = sub.replace(messageTemplate);

    return new ErrorInstance(UUID.randomUUID(), family, scope, code, title, msg);
  }

  /**
   * Creates a {@link ErrorTemplate} by loading content from the {@link #DEFAULT_ERROR_FILE} file.
   *
   * <p>Checks that the `T` class has a constructor that takes a single {@link ErrorInstance}
   * parameter.
   *
   * <p>
   *
   * @param exceptionClass The class of the exception the template creates.
   * @param family The {@link ErrorFamily} the error belongs to.
   * @param scope The {@link ErrorScope} the error belongs to.
   * @param code The SNAKE_CASE error code for the error.
   * @return {@link ErrorTemplate} that the Error Code num can provide to the {@link
   *     io.stargate.sgv2.jsonapi.exception.ErrorCode} interface.
   * @param <T> Type of the {@link APIException} the error code creates.
   */
  public static <T extends APIException> ErrorTemplate<T> load(
      Class<T> exceptionClass, ErrorFamily family, ErrorScope scope, String code) {

    Constructor<T> constructor;
    try {
      constructor = exceptionClass.getConstructor(ErrorInstance.class);
    } catch (NoSuchMethodException | SecurityException e) {
      throw new RuntimeException(
          "Failed to find the APIException.ErrorInstance.class constructor for the exception class"
              + exceptionClass.getSimpleName(),
          e);
    }

    // TODO: This is where we would lookup the error codes from the data in the yaml file.
    // TODO: we also need to work out how the snippets get used in toInstance()
    // TODO: when the scope is missing it will be "" in the code, but the YAML uses NONE
    var snakeCode =
        CharMatcher.whitespace().replaceFrom(Strings.nullToEmpty(code), '_').toUpperCase();

    String title =
        ConfigProvider.getConfig()
            .getValue(
                "errors"
                    + "."
                    + family.name()
                    + "."
                    + scope.safeScope()
                    + "."
                    + snakeCode
                    + "."
                    + "title",
                String.class);
    String body =
        ConfigProvider.getConfig()
            .getValue(
                "errors"
                    + "."
                    + family.name()
                    + "."
                    + scope.safeScope()
                    + "."
                    + snakeCode
                    + "."
                    + "body",
                String.class);

    return new ErrorTemplate<T>(
        constructor,
        family,
        scope,
        scope.safeScope().isBlank() ? code : String.format("%s_%s", scope.safeScope(), code),
        title,
        body);
  }
}
