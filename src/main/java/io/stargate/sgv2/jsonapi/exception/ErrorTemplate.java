package io.stargate.sgv2.jsonapi.exception;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import org.apache.commons.text.StringSubstitutor;

/**
 * A template for creating an {@link APIException}, that is associated with an Error Code enum so
 * the {@link ErrorCodeV1} interface can easily create the exception.
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
 * @param <T> The type of the {@link APIException} the template creates.
 * @param constructor A constructor accepts a single parameter of {@link ErrorInstance} and returns
 *     an instance of the `T` type.
 * @param family {@link ErrorFamily} the error belongs to.
 * @param scope {@link ErrorScope} the error belongs to.
 * @param code SNAKE_CASE error code for the error.
 * @param title Title of the error, does not change between instances.
 * @param messageTemplate A template for the error body, with variables to be replaced at runtime
 *     using the {@link StringSubstitutor} from Apache Commons Text.
 * @param httpStatusOverride If present, overrides the default HTTP 200 response code for errors.
 * @param exceptionFlags The set of exception actions to apply to this error instance.
 */
public record ErrorTemplate<T extends APIException>(
    Constructor<T> constructor,
    ErrorFamily family,
    ErrorScope scope,
    String code,
    String title,
    String messageTemplate,
    Optional<Integer> httpStatusOverride,
    EnumSet<ExceptionFlags> exceptionFlags) {

  public static final String NULL_REPLACEMENT = "(null)";

  /**
   * The Apache text substitution will throw an error if the substitution value is null, call this
   * method to ensure the text value is not a null.
   *
   * @param value String value that may be a null.
   * @return The value or {@link #NULL_REPLACEMENT} if the value is null.
   */
  public static String replaceIfNull(String value) {
    return value == null ? NULL_REPLACEMENT : value;
  }

  public T toException(EnumSet<ExceptionFlags> exceptionFlags, Map<String, String> values) {
    var errorInstance = toInstance(values, exceptionFlags);

    try {
      return constructor().newInstance(errorInstance);
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new IllegalStateException(
          "Failed to create a new instance of " + constructor().getDeclaringClass().getSimpleName(),
          e);
    }
  }

  public T toException(Map<String, String> values) {
    return toException(EnumSet.noneOf(ExceptionFlags.class), values);
  }

  /**
   * Uses the template to create a {@link ErrorInstance} with the provided values.
   *
   * <p>As well as the values provided in the params, the template can use "snippets" that are
   * defined in the {@link ErrorConfig#DEFAULT_ERROR_CONFIG_FILE} file. See the file for infor on
   * how to define them.
   *
   * <p>
   *
   * @param values The values to use in the template for the error body.
   * @param exceptionFlags The set of exception actions to apply to this error instance.
   * @return {@link ErrorInstance} created from the template.
   * @throws UnresolvedErrorTemplateVariable if the template string for the body of the exception
   *     has variables e.g <code>${my_var}</code> that are not in the <code>values</code> passed in
   *     or not in the {@link ErrorConfig#getSnippetVars()} from the error config.
   */
  private ErrorInstance toInstance(
      Map<String, String> values, EnumSet<ExceptionFlags> exceptionFlags) {

    // use the apache string substitution to replace the variables in the messageTemplate
    Map<String, String> allValues = new HashMap<>(values);
    allValues.putAll(ErrorConfig.getInstance().getSnippetVars());

    // the substitution will throw an exception if a variable is null
    // easy way to handle this is to pre-process the map rather than custom substituter
    allValues.replaceAll((k, v) -> replaceIfNull(v));

    // set so IllegalArgumentException thrown if template var missing a value
    var subs = new StringSubstitutor(allValues).setEnableUndefinedVariableException(true);

    String msg;
    try {
      msg = subs.replace(messageTemplate);
    } catch (IllegalArgumentException e) {
      throw new UnresolvedErrorTemplateVariable(this, allValues, e.getMessage());
    }

    return new ErrorInstance(
        UUID.randomUUID(), family, scope, code, title, msg, httpStatusOverride, exceptionFlags);
  }

  /**
   * Creates a {@link ErrorTemplate} by loading content from the {@link
   * ErrorConfig#DEFAULT_ERROR_CONFIG_FILE} file.
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
   * @return {@link ErrorTemplate} that the Error Code num can provide to the {@link ErrorCodeV1}
   *     interface.
   * @param <T> Type of the {@link APIException} the error code creates.
   * @throws IllegalArgumentException if the <code>exceptionClass</code> does not have the
   *     constructor needed.
   */
  public static <T extends APIException> ErrorTemplate<T> load(
      Class<T> exceptionClass, ErrorFamily family, ErrorScope scope, String code) {

    final Constructor<T> constructor;
    try {
      constructor = exceptionClass.getConstructor(ErrorInstance.class);
    } catch (NoSuchMethodException | SecurityException e) {
      throw new IllegalArgumentException(
          "Failed to find constructor that accepts APIException.ErrorInstance.class for the exception class: "
              + exceptionClass.getSimpleName(),
          e);
    }

    var errorConfig =
        ErrorConfig.getInstance()
            .getErrorDetail(family, scope.scope(), code)
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        String.format(
                            "Could not find error config for family=%s, scope=%s, code=%s",
                            family, scope, code)));

    return new ErrorTemplate<>(
        constructor,
        family,
        scope,
        code,
        errorConfig.title(),
        errorConfig.body(),
        errorConfig.httpStatusOverride(),
        EnumSet.noneOf(ExceptionFlags.class));
  }
}
