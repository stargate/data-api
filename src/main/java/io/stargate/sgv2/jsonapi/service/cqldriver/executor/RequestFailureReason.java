package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

/**
 * Roughly copied from `RequestFailureReason` in the Cassandra code base, but simplified to only the
 * codes.
 *
 * <p><b>NOTE:</b> The codes here need to be in sync with the C* code, the exceptions in the drivers
 * only use the codes. Sync with <em>Astra</em> version of the code not the DataStax because the
 * Astra version has more codes.
 *
 * <p>Used with {@link com.datastax.oss.driver.api.core.servererrors.ReadFailureException} and
 * similar errors.
 */
public enum RequestFailureReason {
  // codes must be sequential !
  UNKNOWN(0),
  READ_TOO_MANY_TOMBSTONES(1),
  TIMEOUT(2),
  INCOMPATIBLE_SCHEMA(3),
  INDEX_NOT_AVAILABLE(4),
  UNKNOWN_COLUMN(5),
  UNKNOWN_TABLE(6),
  REMOTE_STORAGE_FAILURE(7);

  public final int code;

  private static final RequestFailureReason[] codeToReasonMap;

  RequestFailureReason(int code) {
    this.code = code;
  }

  static {
    RequestFailureReason[] reasons = values();

    int max = -1;
    for (RequestFailureReason r : reasons) {
      max = Math.max(r.code, max);
    }

    RequestFailureReason[] codeMap = new RequestFailureReason[max + 1];

    for (RequestFailureReason reason : reasons) {
      if (codeMap[reason.code] != null) {
        throw new RuntimeException(
            "Two RequestFailureReason-s that map to the same code: " + reason.code);
      }
      codeMap[reason.code] = reason;
    }

    codeToReasonMap = codeMap;
  }

  public static RequestFailureReason fromCode(int code) {
    if (code < 0) {
      throw new IllegalArgumentException(
          "RequestFailureReason code must be non-negative (got " + code + ')');
    }
    // be forgiving and return UNKNOWN if we aren't aware of the code - for forward compatibility
    return code < codeToReasonMap.length ? codeToReasonMap[code] : UNKNOWN;
  }
}
