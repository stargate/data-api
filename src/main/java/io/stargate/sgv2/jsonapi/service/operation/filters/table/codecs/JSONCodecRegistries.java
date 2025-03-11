package io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs;

import java.util.List;

/**
 * Defines the default {@link JSONCodecRegistry} that and the {@link JSONCodec}s it contains.
 *
 * <p><b>NOTE:</b> Only codecs in {@link #DEFAULT_REGISTRY} will be used by the API
 */
public abstract class JSONCodecRegistries {

  /** The default to be used to convert a Java value into what the CQL driver wants to see. */
  public static final JSONCodecRegistry DEFAULT_REGISTRY;

  /**
   * Specific registry that expects map values to only be a list of the keys, not the map itself.
   * See {@link MapKeyJSONCodecRegistry} for more details.
   */
  public static final JSONCodecRegistry MAP_KEY_REGISTRY;

  static {
    var defaultRegistry =
        new DefaultJSONCodecRegistry(
            List.of(
                // Numeric Codecs, integer types
                JSONCodecs.BIGINT_FROM_BIG_DECIMAL,
                JSONCodecs.BIGINT_FROM_BIG_INTEGER,
                JSONCodecs.BIGINT_FROM_LONG,
                JSONCodecs.COUNTER_FROM_LONG,
                JSONCodecs.INT_FROM_BIG_DECIMAL,
                JSONCodecs.INT_FROM_BIG_INTEGER,
                JSONCodecs.INT_FROM_LONG,
                JSONCodecs.SMALLINT_FROM_BIG_DECIMAL,
                JSONCodecs.SMALLINT_FROM_BIG_INTEGER,
                JSONCodecs.SMALLINT_FROM_LONG,
                JSONCodecs.TINYINT_FROM_BIG_DECIMAL,
                JSONCodecs.TINYINT_FROM_BIG_INTEGER,
                JSONCodecs.TINYINT_FROM_LONG,
                JSONCodecs.VARINT_FROM_BIG_DECIMAL,
                JSONCodecs.VARINT_FROM_BIG_INTEGER,
                JSONCodecs.VARINT_FROM_LONG,
                // Numeric Codecs, floating-point types
                JSONCodecs.DECIMAL_FROM_BIG_DECIMAL,
                JSONCodecs.DECIMAL_FROM_BIG_INTEGER,
                JSONCodecs.DECIMAL_FROM_LONG,
                JSONCodecs.DOUBLE_FROM_BIG_DECIMAL,
                JSONCodecs.DOUBLE_FROM_BIG_INTEGER,
                JSONCodecs.DOUBLE_FROM_LONG,
                JSONCodecs.DOUBLE_FROM_STRING,
                JSONCodecs.FLOAT_FROM_BIG_DECIMAL,
                JSONCodecs.FLOAT_FROM_BIG_INTEGER,
                JSONCodecs.FLOAT_FROM_LONG,
                JSONCodecs.FLOAT_FROM_STRING,
                // Text Codecs
                JSONCodecs.ASCII,
                JSONCodecs.TEXT,
                // Date/Time Codecs
                JSONCodecs.DATE_FROM_STRING,
                JSONCodecs.DURATION_FROM_STRING,
                JSONCodecs.TIME_FROM_STRING,
                JSONCodecs.TIMESTAMP_FROM_EJSON,
                JSONCodecs.TIMESTAMP_FROM_STRING,

                // UUID codecs
                JSONCodecs.UUID_FROM_STRING,
                JSONCodecs.TIMEUUID_FROM_STRING,

                // Other codecs
                JSONCodecs.BINARY,
                JSONCodecs.BOOLEAN,
                JSONCodecs.INET_FROM_STRING));

    DEFAULT_REGISTRY = defaultRegistry;
    MAP_KEY_REGISTRY = new MapKeyJSONCodecRegistry(defaultRegistry);
  }
}
