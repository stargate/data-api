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
  /** Defines the default maximum document size. */
  int DEFAULT_MAX_DOCUMENT_SIZE = 4_000_000;

  /** Defines the default maximum document (nesting) depth */
  int DEFAULT_MAX_DOCUMENT_DEPTH = 16;

  /**
   * Defines the default maximum length (in elements) of a single indexable Array in JSON document.
   */
  int DEFAULT_MAX_ARRAY_LENGTH = 1_000;

  /**
   * Defines the default maximum number of properties for any single indexable Object in JSON
   * document.
   */
  int DEFAULT_MAX_OBJECT_PROPERTIES = 1000;

  /**
   * Defines the default maximum number of indexable properties (properties in indexable Objects)
   * for the whole JSON document (including Object- and Array-valued properties).
   */
  int DEFAULT_MAX_DOC_PROPERTIES = 2000;

  /** Defines the default maximum length of a single Number value (in characters) */
  int DEFAULT_MAX_NUMBER_LENGTH = 100;

  /**
   * Defines the default maximum total length of path to individual properties in JSON documents:
   * composed of individual Object property names separated by dots (".").
   */
  int DEFAULT_MAX_PROPERTY_PATH_LENGTH = 1000;

  /**
   * Defines the default maximum length of a single String value: 8,000 bytes with 1.0.0-BETA-6 and
   * later (16,000 characters before)
   */
  int DEFAULT_MAX_STRING_LENGTH_IN_BYTES = 8_000;

  /**
   * Defines the maximum dimension allowed for {@code $vector} field allowed: defaults to 4096 (as
   * of 1.0.0-BETA-7).
   */
  int DEFAULT_MAX_VECTOR_EMBEDDING_LENGTH = 4096;

  /**
   * @return Defines the maximum document size, defaults to {@code 4 meg} (4 million characters).
   */
  @Positive
  @WithDefault("" + DEFAULT_MAX_DOCUMENT_SIZE)
  int maxSize();

  /**
   * @return Defines the maximum document depth (nesting), defaults to {@code 8 levels}
   */
  @Positive
  @WithDefault("" + DEFAULT_MAX_DOCUMENT_DEPTH)
  int maxDepth();

  /**
   * @return Defines the maximum length of paths to properties in JSON documents, defaults to {@code
   *     250 characters}. Note that this is the total length of the path (sequence of one or more
   *     individual property names separated by comma).
   */
  @Positive
  @WithDefault("" + DEFAULT_MAX_PROPERTY_PATH_LENGTH)
  int maxPropertyPathLength();

  /**
   * @return Defines the maximum number of properties any single indexable Object in JSON document
   *     can contain, defaults to {@code 1,000} (note: this is not the total number of properties in
   *     the whole document but in the main or indexable sub-document)
   */
  @Positive
  @WithDefault("" + DEFAULT_MAX_OBJECT_PROPERTIES)
  int maxObjectProperties();

  /**
   * @return Defines the maximum number of indexable properties in JSON document can contain,
   *     defaults to {@code 2,000}, including all indexable Object- and Array-valued properties.
   */
  @Positive
  @WithDefault("" + DEFAULT_MAX_DOC_PROPERTIES)
  int maxDocumentProperties();

  /**
   * @return Defines the maximum length of a single Number value (in characters).
   */
  @Positive
  @WithDefault("" + DEFAULT_MAX_NUMBER_LENGTH)
  int maxNumberLength();

  /**
   * @return Defines the maximum length of a single String value (in bytes).
   */
  @Positive
  @WithDefault("" + DEFAULT_MAX_STRING_LENGTH_IN_BYTES)
  int maxStringLengthInBytes();

  /**
   * @return Maximum length of an indexable Array in document (in elements).
   */
  @Positive
  @WithDefault("" + DEFAULT_MAX_ARRAY_LENGTH)
  int maxArrayLength();

  /**
   * @return Maximum length of Vector ($vector) array allowed.
   */
  @Positive
  @WithDefault("" + DEFAULT_MAX_VECTOR_EMBEDDING_LENGTH)
  int maxVectorEmbeddingLength();
}
