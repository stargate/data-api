package io.stargate.sgv2.jsonapi.util;

import com.fasterxml.jackson.core.Base64Variants;

/**
 * Helper methods for Base64 encoding and decoding, using the MIME variant without line feeds (see
 * <a href="https://en.wikipedia.org/wiki/Base64">Base64</a> for more information).
 */
public interface Base64Util {
  static byte[] decodeFromMimeBase64(String encoded) throws IllegalArgumentException {
    return Base64Variants.MIME_NO_LINEFEEDS.decode(encoded);
  }

  static String encodeAsMimeBase64(byte[] binary) {
    return Base64Variants.MIME_NO_LINEFEEDS.encode(binary);
  }
}
