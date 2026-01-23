package io.stargate.sgv2.jsonapi.exception;

import com.google.common.base.Preconditions;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Interface for any enum that represents an error code to implement.
 *
 * <p>This interface is used because multiple ENUM's define the scopes, the interface creates a way
 * to treat the values from the different ENUM's in a consistent way.
 *
 * <p>The interface makes it easy for code to get an instance of the exception the code represents,
 * built using the templates error information in {@link ErrorTemplate} that is loaded at startup.
 *
 * <p>With this interface code creates an instance of the exception by calling any of the {@link
 * #get()} overloads for example:
 *
 * <pre>
 *   FilterException.Code.MULTIPLE_ID_FILTER.get("variable_name", "value");
 *   RequestException.Code.FAKE_CODE.get();
 * </pre>
 *
 * @param <T> Type of the {@link APIException} the error code creates.
 */
public interface ErrorCode<T extends APIException> {
  /** Since these are always implemented as Enums, can and should expose logical name as well */
  String name();

  /**
   * Gets an instance of the {@link APIException} the error code represents without providing any
   * substitution values for the error body.
   *
   * @return Instance of {@link APIException} the error code represents.
   */
  default T get() {
    return get(Map.of());
  }

  /**
   * Gets an instance of the {@link APIException} the error code represents, providing substitution
   * values for the error body as a param array.
   *
   * @param exceptionFlags The set of exception actions to apply to this error instance.
   * @param values Substitution values for the error body. The array length must be a multiple of 2,
   *     each pair of strings is treated as a key-value pair for example ["key-1", "value-1",
   *     "key-2", "value-2"]
   * @return Instance of {@link APIException} the error code represents.
   */
  default T get(EnumSet<ExceptionFlags> exceptionFlags, String... values) {
    Preconditions.checkArgument(
        values.length % 2 == 0, "Length of the values must be a multiple of 2");
    Map<String, String> valuesMap = new HashMap<>(values.length / 2);
    for (int i = 0; i < values.length; i += 2) {
      valuesMap.put(values[i], values[i + 1]);
    }
    return get(exceptionFlags, valuesMap);
  }

  /**
   * Convenience overload that delegates to {@link #get(EnumSet, String...)} with an empty set of
   * {@link ExceptionFlags}.
   */
  default T get(String... values) {
    return get(EnumSet.noneOf(ExceptionFlags.class), values);
  }

  /**
   * Gets an instance of the {@link APIException} the error code represents, providing substitution
   * values for the error body as a param array.
   *
   * @param values May of substitution values for the error body.
   * @return Instance of {@link APIException} the error code represents.
   */
  default T get(Map<String, String> values) {
    return get(EnumSet.noneOf(ExceptionFlags.class), values);
  }

  default T get(EnumSet<ExceptionFlags> exceptionFlags, Map<String, String> values) {
    return template().toException(exceptionFlags, values);
  }

  /**
   * By-pass factory method needed when translating from gRPC into proper exception instance:
   * message is pre-formatted and needs to by-pass formatting.
   */
  default T withPreformattedMessage(String formattedMessage) {
    return template()
        .withPreformattedMessage(EnumSet.noneOf(ExceptionFlags.class), formattedMessage);
  }

  /**
   * ENUM Implementers must return a non-null {@link ErrorTemplate} that is used to build an
   * instance of the Exception the code represents.
   *
   * @return {@link ErrorTemplate} for the error code.
   */
  ErrorTemplate<T> template();
}
