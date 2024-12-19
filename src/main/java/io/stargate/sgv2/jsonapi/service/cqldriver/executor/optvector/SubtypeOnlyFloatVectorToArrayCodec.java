package io.stargate.sgv2.jsonapi.service.cqldriver.executor.optvector;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.VectorType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.codec.FloatCodec;
import com.datastax.oss.driver.shaded.guava.common.base.Splitter;
import com.datastax.oss.driver.shaded.guava.common.collect.Iterators;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;

/**
 * Implementation of {@link TypeCodec} which translates CQL vectors into float arrays. Difference
 * between this and {@link
 * com.datastax.oss.driver.internal.core.type.codec.extras.vector.FloatVectorToArrayCodec} is that
 * we don't concern ourselves with the dimensionality specified in the input CQL type. This codec
 * just reads all the bytes, tries to deserislize them consecutively into subtypes and then returns
 * the result. Serialiation is similar: we take the input array, serialize each element and return
 * the result.
 */
public class SubtypeOnlyFloatVectorToArrayCodec implements TypeCodec<float[]> {

  private static final int ELEMENT_SIZE = 4;

  @NonNull protected final VectorType cqlType;
  @NonNull protected final GenericType<float[]> javaType;

  private final FloatCodec floatCodec = new FloatCodec();

  private static final SubtypeOnlyFloatVectorToArrayCodec INSTANCE =
      new SubtypeOnlyFloatVectorToArrayCodec(DataTypes.FLOAT);

  private SubtypeOnlyFloatVectorToArrayCodec(@NonNull DataType subType) {
    cqlType = new SubtypeOnlyVectorType(Objects.requireNonNull(subType, "subType cannot be null"));
    javaType = GenericType.of(float[].class);
  }

  public static TypeCodec<float[]> instance() {
    return INSTANCE;
  }

  @NonNull
  @Override
  public GenericType<float[]> getJavaType() {
    return javaType;
  }

  @NonNull
  @Override
  public DataType getCqlType() {
    return cqlType;
  }

  @Override
  public boolean accepts(@NonNull Class<?> javaClass) {
    return float[].class.equals(javaClass);
  }

  @Override
  public boolean accepts(@NonNull Object value) {
    return value instanceof float[];
  }

  @Override
  public boolean accepts(@NonNull DataType value) {
    if (!(value instanceof VectorType)) {
      return false;
    }
    VectorType valueVectorType = (VectorType) value;
    return this.cqlType.getElementType().equals(valueVectorType.getElementType());
  }

  @Nullable
  @Override
  public ByteBuffer encode(@Nullable float[] array, @NonNull ProtocolVersion protocolVersion) {
    if (array == null) {
      return null;
    }
    int length = array.length;
    int totalSize = length * ELEMENT_SIZE;
    ByteBuffer output = ByteBuffer.allocate(totalSize);
    for (int i = 0; i < length; i++) {
      serializeElement(output, array, i, protocolVersion);
    }
    output.flip();
    return output;
  }

  @Nullable
  @Override
  public float[] decode(@Nullable ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion) {
    if (bytes == null || bytes.remaining() == 0) {
      throw new IllegalArgumentException(
          "Input ByteBuffer must not be null and must have non-zero remaining bytes");
    }
    // TODO: Do we want to treat this as an error?  We could also just ignore any extraneous bytes
    // if they appear.
    if (bytes.remaining() % ELEMENT_SIZE != 0) {
      throw new IllegalArgumentException(
          String.format("Input ByteBuffer should have a multiple of %d bytes", ELEMENT_SIZE));
    }
    ByteBuffer input = bytes.duplicate();
    int elementCount = input.remaining() / 4;
    float[] array = new float[elementCount];
    for (int i = 0; i < elementCount; i++) {
      deserializeElement(input, array, i, protocolVersion);
    }
    return array;
  }

  /**
   * Write the {@code index}th element of {@code array} to {@code output}.
   *
   * @param output The ByteBuffer to write to.
   * @param array The array to read from.
   * @param index The element index.
   * @param protocolVersion The protocol version to use.
   */
  protected void serializeElement(
      @NonNull ByteBuffer output,
      @NonNull float[] array,
      int index,
      @NonNull ProtocolVersion protocolVersion) {
    output.putFloat(array[index]);
  }

  /**
   * Read the {@code index}th element of {@code array} from {@code input}.
   *
   * @param input The ByteBuffer to read from.
   * @param array The array to write to.
   * @param index The element index.
   * @param protocolVersion The protocol version to use.
   */
  protected void deserializeElement(
      @NonNull ByteBuffer input,
      @NonNull float[] array,
      int index,
      @NonNull ProtocolVersion protocolVersion) {
    array[index] = input.getFloat();
  }

  @NonNull
  @Override
  public String format(@Nullable float[] value) {
    return value == null ? "NULL" : Arrays.toString(value);
  }

  @Nullable
  @Override
  public float[] parse(@Nullable String str) {
    /* TODO: Logic below requires a double traversal through the input String but there's no other obvious way to
     * get the size.  It's still probably worth the initial pass through in order to avoid having to deal with
     * resizing ops.  Fortunately we're only dealing with the format/parse pair here so this shouldn't impact
     * general performance much. */
    if ((str == null) || str.isEmpty()) {
      throw new IllegalArgumentException("Cannot create float array from null or empty string");
    }
    Iterable<String> strIterable =
        Splitter.on(", ").trimResults().split(str.substring(1, str.length() - 1));
    float[] rv = new float[Iterators.size(strIterable.iterator())];
    Iterator<String> strIterator = strIterable.iterator();
    for (int i = 0; i < rv.length; ++i) {
      String strVal = strIterator.next();
      // TODO: String.isBlank() should be included here but it's only available with Java11+
      if (strVal == null || strVal.isEmpty()) {
        throw new IllegalArgumentException("Null element observed in float array string");
      }
      rv[i] = floatCodec.parse(strVal).floatValue();
    }
    return rv;
  }
}
