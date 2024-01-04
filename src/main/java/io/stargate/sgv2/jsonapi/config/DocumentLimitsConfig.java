package io.stargate.sgv2.jsonapi.config;

import io.quarkus.runtime.annotations.StaticInitSafe;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import jakarta.validation.constraints.Positive;

/**
 * Configuration Object that defines limits on Documents managed by JSON API. Needed early for
 * providers so has to be declared as {@link StaticInitSafe}.
 */
@StaticInitSafe
@ConfigMapping(prefix = "stargate.jsonapi.document.limits")
public interface DocumentLimitsConfig {

  /** Defines the default max size of filter fields. */
  int DEFAULT_MAX_FILTER_SIZE = 64;

  /** Defines the default maximum document size. */
  int DEFAULT_MAX_DOCUMENT_SIZE = 1_000_000;

  /** Defines the default maximum length (in elements) of a single Array value */
  int DEFAULT_MAX_ARRAY_LENGTH = 1_000;

  /**
   * Defines the default maximum number of properties any single Object in JSON document can contain
   */
  int DEFAULT_MAX_OBJECT_PROPERTIES = 64;

  /**
   * Defines the default maximum number of properties the whole JSON document can contain (including
   * Object- and Array-valued properties).
   */
  int DEFAULT_MAX_DOC_PROPERTIES = 1000;

  /** Defines the default maximum length of a single Number value (in characters) */
  int DEFAULT_MAX_NUMBER_LENGTH = 50;

  /** Defines the maximum length of property names in JSON documents */
  int DEFAULT_MAX_PROPERTY_NAME_LENGTH = 48;

  /**
   * Defines the default maximum length of a single String value: 8,000 bytes with 1.0.0-BETA-6 and
   * later (16,000 characters before)
   */
  int DEFAULT_MAX_STRING_LENGTH_IN_BYTES = 8_000;

  /**
   * @return Defines the maximum document size, defaults to {@code 1 meg} (1 million characters).
   */
  @Positive
  @WithDefault("" + DEFAULT_MAX_DOCUMENT_SIZE)
  int maxSize();

  /** @return Defines the maximum document depth (nesting), defaults to {@code 8 levels} */
  @Positive
  @WithDefault("8")
  int maxDepth();

  /**
   * @return Defines the maximum length of property names in JSON documents, defaults to {@code 48
   *     characters} (note: length is for individual name segments; full dotted names can be longer)
   */
  @Positive
  @WithDefault("" + DEFAULT_MAX_PROPERTY_NAME_LENGTH)
  int maxPropertyNameLength();

  /**
   * @return Defines the maximum number of properties any single Object in JSON document can
   *     contain, defaults to {@code 64} (note: this is not the total number of properties in the
   *     whole document, only on individual main or sub-document)
   */
  @Positive
  @WithDefault("" + DEFAULT_MAX_OBJECT_PROPERTIES)
  int maxObjectProperties();

  /**
   * @return Defines the maximum number of properties the whole JSON document can contain, defaults
   *     to {@code 1000}, including Object- and Array-valued properties.
   */
  @Positive
  @WithDefault("" + DEFAULT_MAX_DOC_PROPERTIES)
  int maxDocumentProperties();

  /**
   * @return Defines the max size of filter fields, defaults to {@code 64}, which is tha same as the
   *     maximum number of properties of a single Json object. (note: this does not count the fields
   *     in '$operation' such as $in, $all)
   */
  @Positive
  @WithDefault("" + DEFAULT_MAX_FILTER_SIZE)
  int maxFilterObjectProperties();

  /** @return Defines the maximum length of a single Number value (in characters). */
  @Positive
  @WithDefault("" + DEFAULT_MAX_NUMBER_LENGTH)
  int maxNumberLength();

  /** @return Defines the maximum length of a single String value (in bytes). */
  @Positive
  @WithDefault("" + DEFAULT_MAX_STRING_LENGTH_IN_BYTES)
  int maxStringLengthInBytes();

  /** @return Maximum length of an Array in document, defaults to {@code 1,000} elements. */
  @Positive
  @WithDefault("" + DEFAULT_MAX_ARRAY_LENGTH)
  int maxArrayLength();

  /**
   * @return Maximum length of Vector ($vector) array JSON API allows -- NOTE: backend data store
   *     may limit length to a lower value; but we want to prevent handling of huge arrays before
   *     trying to pass them to DB. Or, conversely, if data store does not limit length, to impose
   *     something reasonable from JSON API perspective (for service-protection reasons).
   */
  @Positive
  @WithDefault("16000")
  int maxVectorEmbeddingLength();
}
