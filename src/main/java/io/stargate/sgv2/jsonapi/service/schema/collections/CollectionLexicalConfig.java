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

  public CollectionLexicalConfig(boolean enabled, JsonNode analyzerDefinition) {
    this.enabled = enabled;
    // Clear out any analyzer settings if not enabled (but don't fail)
    if (enabled) {
      this.analyzerDefinition = Objects.requireNonNull(analyzerDefinition);
    } else {
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
      boolean lexicalAvailable,
      CreateCollectionCommand.Options.LexicalConfigDefinition lexicalConfig) {
    // If not defined, enable if available, otherwise disable
    if (lexicalConfig == null) {
      return lexicalAvailable ? configForEnabledStandard() : configForDisabled();
    }
    // Otherwise validate and construct
    Boolean enabled = lexicalConfig.enabled();
    if (enabled == null) {
      throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
          "'enabled' is required property for 'lexical' Object value");
    }
    // Can only enable if feature is available
    if (enabled && !lexicalAvailable) {
      throw ErrorCodeV1.LEXICAL_NOT_AVAILABLE_FOR_DATABASE.toApiException();
    }

    JsonNode analyzer = lexicalConfig.analyzerDef();
    if (analyzer == null) {
      analyzer = mapper.getNodeFactory().textNode(CollectionLexicalConfig.DEFAULT_NAMED_ANALYZER);
    } else if (!analyzer.isTextual() && !analyzer.isObject()) {
      throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
          "'analyzer' property of 'lexical' must be either String or JSON Object, is: %s",
          analyzer.getNodeType());
    }
    return new CollectionLexicalConfig(enabled.booleanValue(), analyzer);
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
