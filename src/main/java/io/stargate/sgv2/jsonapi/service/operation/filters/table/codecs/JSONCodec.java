package io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import java.util.function.BiPredicate;
import java.util.function.Function;

/**
 * Handles the conversation between the in memory Java representation of a value from a JSON
 * document and the Java type that the driver expects for the CQL type of the column.
 *
 * <p>This is codec sitting above the codec the Java C* driver uses.
 *
 * <p>The path is:
 *
 * <ul>
 *   <li>JSON Document
 *   <li>Jackson parses and turns into Java Object (e.g. BigInteger)
 *   <li>JSONCodec (this class) turns Java Object into the Java type the C* driver expects (e.g.
 *       Short
 *   <li>C* driver codec turns Java type into C* type
 * </ul>
 *
 * TODO: expand this idea to be map to and from the CQL representation, we can use it to build the
 * JSON doc from reading a row and to use it for writing a row.
 *
 * @param javaType {@link GenericType} of the Java object that needs to be transformed into the type
 *     CQL expects.
 * @param targetCQLType {@link DataType} of the CQL column type the Java object needs to be
 *     transformed into.
 * @param fromJava Function that transforms the Java object into the CQL object
 * @param <JavaT> The type of the Java object that needs to be transformed into the type CQL expects
 * @param <CqlT> The type Java object the CQL driver expects
 */
public record JSONCodec<JavaT, CqlT>(
    GenericType<JavaT> javaType, DataType targetCQLType, FromJava<JavaT, CqlT> fromJava)
    implements BiPredicate<DataType, Object> {

  /**
   * Call to check if this codec can convert the type of the `value` into the type needed for a
   * column of the `targetCQLType`.
   *
   * <p>Used to filter the list of codecs to find one that works, which can then be unchecked cast
   * using {@link JSONCodec#unchecked(JSONCodec)}
   *
   * @param targetCQLType {@link DataType} of the CQL column the value will be written to.
   * @param value Instance of a Java value that will be written to the column.
   * @return True if the codec can convert the value into the type needed for the column.
   */
  @Override
  public boolean test(DataType targetCQLType, Object value) {
    // java value tests comes from TypeCodec.accepts(Object value) in the driver
    return this.targetCQLType.equals(targetCQLType)
        && javaType.getRawType().isAssignableFrom(value.getClass());
  }

  /**
   * Applies the codec to the value.
   *
   * @param value Json value of type {@link JavaT} that needs to be transformed into the type CQL
   *     expects.
   * @return Value of type {@link CqlT} that the CQL driver expects.
   * @throws FromJavaCodecException if there was an error converting the value.
   */
  public CqlT apply(JavaT value) throws FromJavaCodecException {
    return fromJava.apply(value, targetCQLType);
  }

  @SuppressWarnings("unchecked")
  public static <JavaT, CqlT> JSONCodec<JavaT, CqlT> unchecked(JSONCodec<?, ?> codec) {
    return (JSONCodec<JavaT, CqlT>) codec;
  }

  /**
   * Function interface that is used by the codec to convert the Java value to the value CQL
   * expects.
   *
   * <p>The interface is used so the conversation function can throw the checked {@link
   * FromJavaCodecException} and the function is also passed the target type so it can construct a
   * better exception.
   *
   * <p>Use the static constructors on the interface to get instances, see it's use in the {@link
   * JSONCodecRegistry}
   *
   * @param <T> The type of the Java object that needs to be transformed into the type CQL expects
   * @param <R> The type Java object the CQL driver expects
   */
  @FunctionalInterface
  public interface FromJava<T, R> {

    /**
     * Convers the current Java value to the type CQL expects.
     *
     * @param t
     * @param targetType The type of the CQL column the value will be written to, passed so it can
     *     be used when creating an exception if there was a error doing the transformation.
     * @return
     * @throws FromJavaCodecException
     */
    R apply(T t, DataType targetType) throws FromJavaCodecException;

    /**
     * Returns an instance that just returns the value passed in, the same as {@link
     * Function#identity()}
     *
     * <p>Unsafe because it does not catch any errors from the conversion, because there are none.
     *
     * @return
     * @param <T>
     */
    static <T> FromJava<T, T> unsafeIdentity() {
      return (t, targetType) -> t;
    }

    /**
     * Returns an instance that converts the value to the target type, catching any arithmetic
     * exceptions and throwing them as a {@link FromJavaCodecException}
     *
     * @param function the function that does the conversion, it is expected it may throw a {@link
     *     ArithmeticException}
     * @return
     * @param <T>
     * @param <R>
     */
    static <T extends Number, R> FromJava<T, R> safeNumber(Function<T, R> function) {
      return (t, targetType) -> {
        try {
          return function.apply(t);
        } catch (ArithmeticException e) {
          throw new FromJavaCodecException(t, targetType, e);
        }
      };
    }
  }
}
