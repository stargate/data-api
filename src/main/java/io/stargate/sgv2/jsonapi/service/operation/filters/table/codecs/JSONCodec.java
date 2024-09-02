package io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
 * JSON doc from reading a row and to use it for writing a row. // TODO Mahesh, The codec looks fine
 * for primitive type. Needs a revisit when we doing complex // types where only few fields will
 * need to be returned. Will we be creating custom Codec based // on user requests?
 *
 * @param javaType {@link GenericType} of the Java object that needs to be transformed into the type
 *     CQL expects.
 * @param targetCQLType {@link DataType} of the CQL column type the Java object needs to be
 *     transformed into.
 * @param toCQL Function that transforms the Java object into the CQL object
 * @param toJSON Function that transforms the value returned by CQL into a JsonNode
 * @param <JavaT> The type of the Java object that needs to be transformed into the type CQL expects
 * @param <CqlT> The type Java object the CQL driver expects
 */
public record JSONCodec<JavaT, CqlT>(
    GenericType<JavaT> javaType,
    DataType targetCQLType,
    ToCQL<JavaT, CqlT> toCQL,
    ToJSON<CqlT> toJSON) {

  /**
   * Call to check if this codec can convert the type of the `value` into the type needed for a
   * column of the `toCQLType`.
   *
   * <p>Used to filter the list of codecs to find one that works, which can then be unchecked cast
   * using {@link JSONCodec#unchecked(JSONCodec)}
   *
   * @param toCQLType {@link DataType} of the CQL column the value will be written to.
   * @param value Instance of a Java value that will be written to the column.
   * @return True if the codec can convert the value into the type needed for the column.
   */
  public boolean testToCQL(DataType toCQLType, Object value) {
    // null is convertible  to every tyoe
    if (value == null) {
      return true;
    }
    // java value tests comes from TypeCodec.accepts(Object value) in the driver
    return this.targetCQLType.equals(toCQLType)
        && javaType.getRawType().isAssignableFrom(value.getClass());
  }

  /**
   * Applies the codec to the Java value read from a JSON document to convert it int the value the
   * CQL driver expects.
   *
   * @param value Json value of type {@link JavaT} that needs to be transformed into the type CQL
   *     expects.
   * @return Value of type {@link CqlT} that the CQL driver expects.
   * @throws ToCQLCodecException if there was an error converting the value.
   */
  public CqlT toCQL(JavaT value) throws ToCQLCodecException {
    return toCQL.apply(targetCQLType, value);
  }

  /**
   * Test if this codec can convert the CQL value into a JSON node.
   *
   * <p>See help for {@link #testToCQL(DataType, Object)}
   *
   * @param fromCQLType
   * @return
   */
  public boolean testToJSON(DataType fromCQLType) {
    return this.targetCQLType.equals(fromCQLType);
  }

  /**
   * Applies the codec to the value read from the CQL Driver to create a JSON node representation of
   * it.
   *
   * @param objectMapper {@link ObjectMapper} the codec should use if it needs one.
   * @param value The value read from the CQL driver that needs to be transformed into a {@link
   *     JsonNode}
   * @return {@link JsonNode} that represents the value only, this does not include the column name.
   * @throws ToJSONCodecException Checked exception raised if any error happens, users of the codec
   *     should convert this into the appropriate exception for the use case.
   */
  public JsonNode toJSON(ObjectMapper objectMapper, CqlT value) throws ToJSONCodecException {
    return toJSON.apply(objectMapper, targetCQLType, value);
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
   * ToCQLCodecException}, the function is also passed the target type, so it can construct a better
   * exception.
   *
   * <p>Use the static constructors on the interface to get instances, see it's use in the {@link
   * JSONCodecRegistry}
   *
   * @param <JavaT> The type of the Java object that needs to be transformed into the type CQL
   *     expects
   * @param <CqlT> The type Java object the CQL driver expects
   */
  @FunctionalInterface
  public interface ToCQL<JavaT, CqlT> {

    /**
     * Converts the current Java value to the type CQL expects.
     *
     * @param toCQLType The type of the CQL column the value will be written to, passed, so it can
     *     be used when creating an exception if there was a error doing the transformation.
     * @param value The Java value that needs to be transformed into the type CQL expects.
     * @return
     * @throws ToCQLCodecException
     */
    CqlT apply(DataType toCQLType, JavaT value) throws ToCQLCodecException;

    /**
     * Returns an instance that just returns the value passed in, the same as {@link
     * Function#identity()}
     *
     * <p>Unsafe because it does not catch any errors from the conversion, because there are none.
     * TODO what is the point here? Is it for type-casting purpose or why is this needed?
     *
     * @return
     * @param <JavaT>
     */
    static <JavaT> ToCQL<JavaT, JavaT> unsafeIdentity() {
      return (toCQLType, value) -> value;
    }

    /**
     * Returns an instance that converts the value to the target type, catching any arithmetic
     * exceptions and throwing them as a {@link ToCQLCodecException}
     *
     * @param function the function that does the conversion, it is expected it may throw a {@link
     *     ArithmeticException}
     * @return
     * @param <JavaT>
     * @param <CqlT>
     */
    static <JavaT extends Number, CqlT> ToCQL<JavaT, CqlT> safeNumber(
        Function<JavaT, CqlT> function) {
      return (toCQLType, value) -> {
        try {
          return function.apply(value);
        } catch (ArithmeticException e) {
          throw new ToCQLCodecException(value, toCQLType, e);
        }
      };
    }
  }

  /**
   * Function interface that is used by the codec to convert value returned by CQL into a {@link
   * JsonNode} that can be used to construct the response document for a row.
   *
   * <p>The interface is used so the conversation function can throw the checked {@link
   * ToJSONCodecException}, it is also given the CQL data type to make better exceptions.
   *
   * <p>Use the static constructors on the interface to get instances, see it's use in the {@link
   * JSONCodecRegistry}
   *
   * @param <CqlT> The type Java object the CQL driver expects
   */
  @FunctionalInterface
  public interface ToJSON<CqlT> {

    /**
     * Converts the value read from CQL to a {@link JsonNode}
     *
     * @param objectMapper A {@link ObjectMapper} to use to create the {@link JsonNode} if needed.
     * @param fromCQLType The CQL {@link DataType} of the column that was read from CQL.
     * @param value The value that was read from the CQL driver.
     * @return A {@link JsonNode} that represents the value, this is just the value does not include
     *     the column name.
     * @throws ToJSONCodecException Checked exception raised for any error, users of the function
     *     must catch and convert to the appropriate error for the use case.
     */
    JsonNode apply(ObjectMapper objectMapper, DataType fromCQLType, CqlT value)
        throws ToJSONCodecException;

    /**
     * Returns an instance that will call the nodeFactoryMethod, this is typically a function from
     * the {@link com.fasterxml.jackson.databind.node.JsonNodeFactory} that will create the correct
     * type of node.
     *
     * <p>See usage in the {@link JSONCodecRegistry}
     *
     * <p>Unsafe because it does not catch any errors from the conversion.
     *
     * @param nodeFactoryMethod A function that will create a {@link JsonNode} from value of the
     *     {@param CqlT} type.
     * @return
     * @param <CqlT> The type of the Java value the driver returned.
     */
    static <CqlT> ToJSON<CqlT> unsafeNodeFactory(Function<CqlT, JsonNode> nodeFactoryMethod) {
      return (objectMapper, fromCQLType, value) -> nodeFactoryMethod.apply(value);
    }
  }
}
