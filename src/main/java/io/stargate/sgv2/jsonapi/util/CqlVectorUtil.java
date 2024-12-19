package io.stargate.sgv2.jsonapi.util;

import com.datastax.oss.driver.api.core.data.CqlVector;
import java.util.*;

/**
 * Static utility methods for working with vector value representations; specifically with Cassandra
 * {@code float}-based vector types.
 *
 * <p>CQL {@code float} vectors are represented using couple of different value types:
 *
 * <ul>
 *   <li>As {@link CqlVector} instances of {@code Float} values, when passed to/from Java CQL
 *       Client. These are internally {@code List}s of {@code Float} values.
 *   <li>As binary-packed byte arrays, in which each logical {@code Float} value is encoded using 4
 *       bytes (Big-Endian), as encoded by {@link Float#floatToIntBits(float)}. This means that a
 *       vector of {@code 8} {@code Float} values will be represented as a {@code byte[32]} (for
 *       example). This is used as an intermediate representation.
 *   <li>As Base64-encoded Strings, when embedded in JSON payloads: this is Base64 encoding of the
 *       binary-packed byte arrays.
 * </ul>
 */
public interface CqlVectorUtil {
  /**
   * Method for converting binary-packed representation of a CQL {@code float} vector into a raw
   * {@code float[]} array.
   *
   * @param packedBytes binary-packed representation of the vector
   * @return "Raw" {@code float[]} array representing the vector
   */
  static float[] bytesToFloats(byte[] packedBytes) throws IllegalArgumentException {
    final int bytesPerFloat = Float.BYTES;
    final int inputLen = packedBytes.length;

    // Verify that we have a multiple of 4 bytes (float size)
    if ((inputLen & (bytesPerFloat - 1)) != 0) {
      throw new IllegalArgumentException(
          String.format(
              "binary value to decode is not a multiple of %d bytes long (%d bytes)",
              bytesPerFloat, inputLen));
    }
    float[] floats = new float[inputLen / bytesPerFloat];
    for (int in = 0, out = 0; in < inputLen; in += bytesPerFloat) {
      // Convert from Big-endian 4-byte integer to float
      int intBits =
          (packedBytes[in] & 0xFF) << 24
              | (packedBytes[in + 1] & 0xFF) << 16
              | (packedBytes[in + 2] & 0xFF) << 8
              | (packedBytes[in + 3] & 0xFF);
      floats[out++] = Float.intBitsToFloat(intBits);
    }
    return floats;
  }

  /**
   * Method for converting "raw" {@code float[])} representation of a CQL {@code float} vector into
   * a {@link CqlVector}.
   *
   * @param elements raw {@code float}s that represent the vector elements
   * @return {@link CqlVector} instance representing the vector
   */
  static CqlVector<Float> floatsToCqlVector(float[] elements) {
    List<Float> floats = new ArrayList<>(elements.length);
    for (float element : elements) {
      floats.add(element);
    }
    return CqlVector.newInstance(floats);
  }

  /**
   * Method for encoding vector represented by "raw" {@code float[]} array into binary-packed
   * representation.
   *
   * @param elements raw {@code float}s that represent the vector elements
   * @return Binary-packed representation of the vector
   */
  static byte[] floatsToBytes(float[] elements) {
    int bytesPerFloat = Float.BYTES;
    byte[] bytes = new byte[elements.length * bytesPerFloat];
    for (int in = 0, out = 0; in < elements.length; ++in) {
      // Encode each float as 4-byte integer (Big-endian)
      int intBits = Float.floatToIntBits(elements[in]);
      bytes[out++] = (byte) (intBits >> 24);
      bytes[out++] = (byte) (intBits >> 16);
      bytes[out++] = (byte) (intBits >> 8);
      bytes[out++] = (byte) intBits;
    }
    return bytes;
  }
}
