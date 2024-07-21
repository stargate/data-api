package io.stargate.sgv2.jsonapi.exception.playing;

import com.google.common.base.Preconditions;
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

  /**
   * Gets an instance of the {@link APIException} the error code represents without providing any
   * substitution values for the error message.
   *
   * @return Instance of {@link APIException} the error code represents.
   */
  default T get() {
    return get(Map.of());
  }

  /**
   * Gets an instance of the {@link APIException} the error code represents, providing substitution
   * values for the error message as a param array.
   *
   * @param values Substitution values for the error message. The array length must be a multiple of
   *     2, each pair of strings is treated as a key-value pair for example ["key-1", "value-1",
   *     "key-2", "value-2"]
   * @return Instance of {@link APIException} the error code represents.
   */
  default T get(String... values) {
    Preconditions.checkArgument(
        values.length % 2 == 0, "Length of hte values must be a multiple of 2");
    Map<String, String> valuesMap = new HashMap<>(values.length / 2);
    for (int i = 0; i < values.length; i += 2) {
      valuesMap.put(values[i], values[i + 1]);
    }
    return get(valuesMap);
  }

  /**
   * Gets an instance of the {@link APIException} the error code represents, providing substitution
   * values for the error message as a param array.
   *
   * @param values May of substitution values for the error message.
   * @return Instance of {@link APIException} the error code represents.
   */
  default T get(Map<String, String> values) {
    return template().toException(values);
  }

  /**
   * ENUM Implementers must return a non-null {@link ErrorTemplate} that is used to build an
   * instance of the Exception the code represents.
   *
   * @return {@link ErrorTemplate} for the error code.
   */
  ErrorTemplate<T> template();
}
