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
  /**
   * @return Defines the maximum document page size, defaults to {@code 1 meg} (1 million
   *     characters).
   */
  @Positive
  @WithDefault("1000000")
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
  @WithDefault("48")
  int maxPropertyNameLength();

  /**
   * @return Defines the maximum number of properties any single Object in JSON document can
   *     contain, defaults to {@code 64} (note: this is not the total number of properties in the
   *     whole document, only on individual main or sub-document)
   */
  @Positive
  @WithDefault("64")
  int maxObjectProperties();

  /** @return Defines the maximum length of a single Number value (in characters). */
  @Positive
  @WithDefault("50")
  int maxNumberLength();

  /** @return Defines the maximum length of a single String value. */
  @Positive
  @WithDefault("16000")
  int maxStringLength();

  /** @return Maximum length of an array. */
  @Positive
  @WithDefault("100")
  int maxArrayLength();

  /**
   * @return Maximum length of Vector ($vector) array JSON API allows -- NOTE: backend data store
   *     likely limits length to a lower value; but we want to prevent handling of huge arrays
   *     before trying to pass them to DB.
   */
  @Positive
  @WithDefault("16000")
  int maxVectorEmbeddingLength();
}
