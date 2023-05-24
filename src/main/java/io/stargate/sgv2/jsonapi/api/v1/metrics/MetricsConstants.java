package io.stargate.sgv2.jsonapi.api.v1.metrics;

import io.micrometer.core.instrument.Tag;

public class MetricsConstants {

  // same as V1 io.stargate.core.metrics.StargateMetricConstants#UNKNOWN
  public static final String UNKNOWN_VALUE = "unknown";

  public static final Tag ERROR_TRUE_TAG = Tag.of("error", "true");

  public static final Tag ERROR_FALSE_TAG = Tag.of("error", "false");

  public static final Tag MODULE_TAG = Tag.of("module", "jsonapi");

  public static final String ERROR_CLASS = "errorClass";

  public static final String ERROR_CODE = "errorCode";

  public static final String TENANT = "tenant";

  public static final String COMMAND = "command";

  public static final String NA = "NA";

  public static final Tag DEFAULT_ERROR_CLASS_TAG = Tag.of(ERROR_CLASS, NA);

  public static final Tag DEFAULT_ERROR_CODE_TAG = Tag.of(ERROR_CODE, NA);

  public static final String COUNT_METRICS_NAME = "command_processor_count";

  public static final String TIMER_METRICS_NAME = "command_processor_total";
}
