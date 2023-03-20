package io.stargate.sgv2.jsonapi.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import javax.validation.constraints.Positive;

/** Configuration Object that defines limits on Documents managed by JSON API. */
@ConfigMapping(prefix = "stargate.jsonapi.doc-limits")
public interface DocumentLimitsConfig {
  /**
   * @return Defines the maximum document page size, defaults to {@code 1 meg} (1 million
   *     characters).
   */
  @Positive
  @WithDefault("1000000")
  int maxDocSize();

  /** @return Defines the maximum document depth (nesting), defaults to {@code 8 levels} */
  @Positive
  @WithDefault("8")
  int maxDocDepth();

  /**
   * @return Defines the maximum length of property names in JSON documents, defaults to {@code 48
   *     characters} (note: length is for individual name segments; full dotted names can be longer)
   */
  @Positive
  @WithDefault("48")
  int maxNameLength();

  /**
   * @return Defines the maximum number of properties any single Object in JSON document can
   *     contain, defaults to {@code 64} (note: this is not the total number of properties in the
   *     whole document, only on individual main or sub-document)
   */
  @Positive
  @WithDefault("64")
  int maxObjectProperties();

  /** @return Defines the maximum length of , defaults to {@code 8 levels} */
  @Positive
  @WithDefault("16000")
  int maxStringLength();

  @Positive
  @WithDefault("100")
  int maxArrayLength();
}
