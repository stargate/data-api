package io.stargate.sgv2.jsonapi.metrics;

import io.micrometer.core.instrument.Tag;
import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.util.ClassUtils;
import java.util.List;

public class ExceptionMetrics {

  public static final String TAG_NAME_ERROR = "error";
  public static final String TAG_NAME_ERROR_CLASS = "error.class";
  public static final String TAG_NAME_ERROR_CODE = "error.code";

  private static final String NOT_APPLICABLE = "NA";

  // Re-usable Tags
  public static final Tag TAG_ERROR_TRUE = Tag.of(TAG_NAME_ERROR, "true");
  public static final Tag TAG_ERROR_FALSE = Tag.of(TAG_NAME_ERROR, "false");
  public static final Tag TAG_ERROR_CODE_NOT_APPLICABLE =
      Tag.of(TAG_NAME_ERROR_CODE, NOT_APPLICABLE);
  public static final Tag TAG_ERROR_CLASS_NOT_APPLICABLE =
      Tag.of(TAG_NAME_ERROR_CLASS, NOT_APPLICABLE);

  public static List<Tag> tagsFor(APIException apiException) {

    // These tags must be backwards compatible with how we tracked before
    return List.of(
        Tag.of(TAG_NAME_ERROR_CODE, apiException.fullyQualifiedCode()),
        exceptionClassTag(apiException));
  }

  public static List<Tag> tagsFor(JsonApiException legacyException) {

    // These tags must be backwards compatible with how we tracked before
    // These tags must be backwards compatible with how we tracked before
    return List.of(
        Tag.of(TAG_NAME_ERROR_CODE, legacyException.getFullyQualifiedErrorCode()),
        exceptionClassTag(legacyException));
  }

  private static Tag exceptionClassTag(Throwable throwable) {
    return Tag.of(TAG_NAME_ERROR_CLASS, ClassUtils.classSimpleName(throwable.getClass()));
  }
}
