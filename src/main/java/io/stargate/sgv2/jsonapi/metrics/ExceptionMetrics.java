package io.stargate.sgv2.jsonapi.metrics;

import io.micrometer.core.instrument.Tag;
import io.stargate.sgv2.jsonapi.exception.APIException;
import java.util.List;

public class ExceptionMetrics {

  public static final String TAG_NAME_ERROR = "error";
  public static final String TAG_NAME_ERROR_CODE = "error.code";

  private static final String NOT_APPLICABLE = "NA";

  // Re-usable Tags
  public static final Tag TAG_ERROR_TRUE = Tag.of(TAG_NAME_ERROR, "true");
  public static final Tag TAG_ERROR_FALSE = Tag.of(TAG_NAME_ERROR, "false");
  public static final Tag TAG_ERROR_CODE_NOT_APPLICABLE =
      Tag.of(TAG_NAME_ERROR_CODE, NOT_APPLICABLE);

  public static List<Tag> tagsFor(APIException apiException) {

    // These tags must be backwards compatible with how we tracked before
    return List.of(Tag.of(TAG_NAME_ERROR_CODE, apiException.fullyQualifiedCode()));
  }
}
