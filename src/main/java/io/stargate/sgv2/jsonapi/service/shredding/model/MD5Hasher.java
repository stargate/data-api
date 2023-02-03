package io.stargate.sgv2.jsonapi.service.shredding.model;

import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public class MD5Hasher {
  /**
   * MD5 hash itself is 16 bytes long but Base64-encoding makes it 22 if (and only if!) omitting
   * padding.
   */
  public static int BASE64_ENCODED_MD5_LEN = 22;

  private static final MD5Hasher INSTANCE = new MD5Hasher();

  /**
   * We will use "Basic" Base64 encoder but without padding: this saves 2 bytes of storage space for
   * our use case where alignment is not needed.
   */
  private final Base64.Encoder BASE64_ENCODER = Base64.getEncoder().withoutPadding();

  private MD5Hasher() {}

  /**
   * Method that will calculate MD5 hash for UTF-8 encoded bytes of the input String, and then
   * Base64 encoded it using "Basic" Base64 encoder, resulting in a 22-character ASCII String that
   * is returned
   *
   * @param value String to calculate hash for
   * @return Base64-encoded MD5 hash of given input String
   */
  public static String hashAndBase64Encode(String value) {
    return INSTANCE.hashAndEncode(value);
  }

  private String hashAndEncode(String value) {
    MessageDigest md;

    try {
      md = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      // should never happen but:
      throw new JsonApiException(ErrorCode.SHRED_NO_MD5, e);
    }
    byte[] digest = md.digest(value.getBytes(StandardCharsets.UTF_8));
    return BASE64_ENCODER.encodeToString(digest);
  }
}
