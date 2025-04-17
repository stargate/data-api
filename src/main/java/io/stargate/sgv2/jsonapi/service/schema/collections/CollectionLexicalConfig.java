package io.stargate.sgv2.jsonapi.service.schema.collections;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.util.JsonUtil;
import java.util.Objects;

/** Validated configuration Object for Lexical (BM-25) indexing configuration for Collections. */
public record CollectionLexicalConfig(
    boolean enabled,
    @JsonInclude(JsonInclude.Include.NON_NULL) @JsonProperty("analyzer")
        JsonNode analyzerDefinition) {
  public static final String DEFAULT_NAMED_ANALYZER = "standard";

  private static final JsonNode DEFAULT_NAMED_ANALYZER_NODE =
      JsonNodeFactory.instance.textNode(DEFAULT_NAMED_ANALYZER);

  private static final CollectionLexicalConfig DEFAULT_CONFIG =
      new CollectionLexicalConfig(true, DEFAULT_NAMED_ANALYZER_NODE);

  private static final CollectionLexicalConfig MISSING_CONFIG =
      new CollectionLexicalConfig(false, null);

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
        boolean isAcceptableWhenDisabled =
            analyzerDefinition.isNull()
                || (analyzerDefinition.isObject() && analyzerDefinition.isEmpty());
        if (!isAcceptableWhenDisabled) {
          throw new IllegalArgumentException(
              "Analyzer definition should be omitted, JSON null, or an empty JSON object {} if if lexical is disabled.");
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
      return lexicalAvailableForDB ? configForDefault() : configForDisabled();
    }

    // Case 2: Validate 'enabled' flag is present
    Boolean enabled = lexicalConfig.enabled();
    if (enabled == null) {
      throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
          "'enabled' is required property for 'lexical' Object value");
    }

    // Case 3: Lexical is disabled - ensure analyzer is absent, JSON null, or empty object {}
    if (!enabled) {
      if (lexicalConfig.analyzerDef() != null) {
        // Define the acceptable states when lexical is disabled:
        // 1. The JSON value itself is null (`null`)
        // 2. The JSON value is an empty object (`{}`)
        boolean isAcceptableWhenDisabled =
            lexicalConfig.analyzerDef().isNull()
                || (lexicalConfig.analyzerDef().isObject()
                    && lexicalConfig.analyzerDef().isEmpty());

        if (!isAcceptableWhenDisabled) {
          String nodeType = JsonUtil.nodeTypeAsString(lexicalConfig.analyzerDef());
          throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
              "'lexical' is disabled, but 'lexical.analyzer' property was provided with an unexpected type: %s. "
                  + "When 'lexical' is disabled, 'lexical.analyzer' must either be omitted, JSON null, or an empty JSON object {}.",
              nodeType);
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
    // Case 5a: missing/null/Empty Object - use default analyzer
    if (analyzer == null || analyzer.isNull() || (analyzer.isObject() && analyzer.isEmpty())) {
      analyzer = mapper.getNodeFactory().textNode(CollectionLexicalConfig.DEFAULT_NAMED_ANALYZER);
    } else if (analyzer.isTextual()) {
      // Case 5b: JSON String - use as-is -- Could/should we try to validate analyzer name?
      ;
    } else if (analyzer.isObject()) {
      // Case 5c: JSON Object - use as-is  -- TODO? validate analyzer wrt required fields?
      ;
    } else {
      // Otherwise, invalid definition
      throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
          "'analyzer' property of 'lexical' must be either JSON Object or String, is: %s",
          JsonUtil.nodeTypeAsString(analyzer));
    }
    return new CollectionLexicalConfig(true, analyzer);
  }

  /**
   * Accessor for an instance to use for "lexical disabled" Collections (but not for ones pre-dating
   * lexical search feature).
   */
  public static CollectionLexicalConfig configForDisabled() {
    return new CollectionLexicalConfig(false, null);
  }

  /**
   * Accessor for a singleton instance used to represent case of default lexical configuration for
   * newly created Collections that do not specify lexical configuration.
   */
  public static CollectionLexicalConfig configForDefault() {
    return DEFAULT_CONFIG;
  }

  /**
   * Accessor for a singleton instance used to represent case of missing lexical configuration for
   * legacy Collections created before lexical search was available.
   */
  public static CollectionLexicalConfig configForPreLexical() {
    return MISSING_CONFIG;
  }
}
