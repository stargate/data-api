package io.stargate.sgv2.jsonapi.util;

import com.datastax.oss.driver.api.core.data.CqlVector;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Static utility methods for working with vector value representations; specifically with Cassandra
 * {@code float}-based vector types.
 */
public interface CqlVectorUtil {
  static CqlVector<Float> bytesToCqlVector(byte[] packedBytes) {
    return floatsToCqlVector(bytesToFloats(packedBytes));
  }

  static float[] bytesToFloats(byte[] packedBytes) throws IllegalArgumentException {
    final int bytesPerFloat = Float.BYTES;
    final int inputLen = packedBytes.length;

    if ((inputLen & (bytesPerFloat - 1)) != 0) {
      throw new IllegalArgumentException(
          String.format(
              "binary value to decode is not a multiple of %d bytes long (%d bytes)",
              bytesPerFloat, inputLen));
    }
    float[] floats = new float[inputLen / bytesPerFloat];
    for (int in = 0, out = 0; in < inputLen; in += bytesPerFloat) {
      int intBits =
          (packedBytes[in] & 0xFF) << 24
              | (packedBytes[in + 1] & 0xFF) << 16
              | (packedBytes[in + 2] & 0xFF) << 8
              | (packedBytes[in + 3] & 0xFF);
      floats[out++] = Float.intBitsToFloat(intBits);
    }
    return floats;
  }

  static CqlVector<Float> floatsToCqlVector(float[] elements) {
    List<Float> floats = new ArrayList<>(elements.length);
    for (float element : elements) {
      floats.add(element);
    }
    return CqlVector.newInstance(floats);
  }

  static ByteBuffer floatsToByteBuffer(float[] elements) {
    return ByteBuffer.wrap(floatsToBytes(elements));
  }

  static byte[] floatsToBytes(float[] elements) {
    int bytesPerFloat = Float.BYTES;
    byte[] bytes = new byte[elements.length * bytesPerFloat];
    for (int in = 0, out = 0; in < elements.length; ++in) {
      int intBits = Float.floatToIntBits(elements[in]);
      bytes[out++] = (byte) (intBits >> 24);
      bytes[out++] = (byte) (intBits >> 16);
      bytes[out++] = (byte) (intBits >> 8);
      bytes[out++] = (byte) intBits;
    }
    return bytes;
  }
}
