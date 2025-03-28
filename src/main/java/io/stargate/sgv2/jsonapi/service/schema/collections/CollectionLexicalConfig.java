package io.stargate.sgv2.jsonapi.service.schema.collections;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import java.util.Objects;

/** Validated configuration Object for Lexical (BM-25) indexing configuration for Collections. */
public record CollectionLexicalConfig(
    boolean enabled,
    @JsonInclude(JsonInclude.Include.NON_NULL) @JsonProperty("analyzer")
        JsonNode analyzerDefinition) {
  public static final String DEFAULT_NAMED_ANALYZER = "standard";

  private static final JsonNode DEFAULT_NAMED_ANALYZER_NODE =
      JsonNodeFactory.instance.textNode(DEFAULT_NAMED_ANALYZER);

  /**
   * Constructs a lexical configuration with the specified enabled state and analyzer definition.
   *
   * <p>Validation behavior:
   *
   * <ul>
   *   <li>If lexical search is enabled (enabled = true), the analyzer definition must not be null
   *   <li>If lexical search is disabled (enabled = false), the analyzer definition must be null or
   *       empty
   * </ul>
   *
   * @param enabled Whether lexical search is enabled for this collection
   * @param analyzerDefinition The JSON configuration for the analyzer, must not be null if lexical
   *     search is enabled and must be null or empty if lexical search is disabled
   * @throws NullPointerException if lexical search is enabled and analyzerDefinition is null
   * @throws IllegalStateException if lexical search is disabled and analyzerDefinition is not null
   */
  public CollectionLexicalConfig(boolean enabled, JsonNode analyzerDefinition) {
    this.enabled = enabled;
    if (enabled) {
      this.analyzerDefinition = Objects.requireNonNull(analyzerDefinition);
    } else {
      if (analyzerDefinition != null) {
        if (analyzerDefinition.isTextual()) {
          throw new IllegalArgumentException(
              "Analyzer definition should not have string if lexical is disabled");
        }
        if (analyzerDefinition.isObject() && !analyzerDefinition.isEmpty()) {
          throw new IllegalArgumentException(
              "Analyzer definition should be null or empty JSON if lexical is disabled");
        }
      }
      this.analyzerDefinition = null;
    }
  }

  /**
   * Method for validating the lexical config passed and constructing actual configuration object to
   * use.
   *
   * @return Valid CollectionLexicalConfig object
   */
  public static CollectionLexicalConfig validateAndConstruct(
      ObjectMapper mapper,
      boolean lexicalAvailableForDB,
      CreateCollectionCommand.Options.LexicalConfigDefinition lexicalConfig) {
    // Case 1: No lexical body provided - use defaults if available, otherwise disable
    if (lexicalConfig == null) {
      return lexicalAvailableForDB ? configForEnabledStandard() : configForDisabled();
    }

    // Case 2: Validate 'enabled' flag is present
    Boolean enabled = lexicalConfig.enabled();
    if (enabled == null) {
      throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
          "'enabled' is required property for 'lexical' Object value");
    }

    // Case 3: Lexical is disabled - ensure no analyzer is provided
    if (!enabled) {
      if (lexicalConfig.analyzerDef() != null) {
        if (lexicalConfig.analyzerDef().isTextual()) {
          throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
              "'lexical' is disabled, but 'lexical.analyzer' property is provided with value: '%s'",
              lexicalConfig.analyzerDef().asText());
        }
        if (lexicalConfig.analyzerDef().isObject() && !lexicalConfig.analyzerDef().isEmpty()) {
          throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
              "'lexical' is disabled, but 'lexical.analyzer' property is provided with non-empty JSON Object");
        }
      }
      return configForDisabled();
    }

    // Case 4: Can only enable if feature is available
    if (enabled && !lexicalAvailableForDB) {
      throw ErrorCodeV1.LEXICAL_NOT_AVAILABLE_FOR_DATABASE.toApiException();
    }

    // Case 5: Enabled and analyzer provided - validate and use
    JsonNode analyzer = lexicalConfig.analyzerDef();
    if (analyzer == null) {
      analyzer = mapper.getNodeFactory().textNode(CollectionLexicalConfig.DEFAULT_NAMED_ANALYZER);
    } else if (!analyzer.isTextual() && !analyzer.isObject()) {
      throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
          "'analyzer' property of 'lexical' must be either String or JSON Object, is: %s",
          analyzer.getNodeType());
    }
    return new CollectionLexicalConfig(true, analyzer);
  }

  /**
   * Accessor for an instance to use for "default enabled" cases, using "standard" analyzer
   * configuration: typically used for new collections where lexical search is available.
   */
  public static CollectionLexicalConfig configForEnabledStandard() {
    return new CollectionLexicalConfig(true, DEFAULT_NAMED_ANALYZER_NODE);
  }

  /**
   * Accessor for an instance to use for "lexical disabled" cases: either for existing collections
   * without lexical config, or envi
   */
  public static CollectionLexicalConfig configForDisabled() {
    return new CollectionLexicalConfig(false, null);
  }
}
