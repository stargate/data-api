package io.stargate.sgv2.jsonapi.util;

import com.fasterxml.jackson.core.Base64Variants;

public interface Base64Util {
  static byte[] decodeFromMimeBase64(String encoded) throws IllegalArgumentException {
    return Base64Variants.MIME_NO_LINEFEEDS.decode(encoded);
  }

  static String encodeAsMimeBase64(byte[] binary) {
    return Base64Variants.MIME_NO_LINEFEEDS.encode(binary);
  }
}
