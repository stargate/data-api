package io.stargate.sgv2.jsonapi.service.schema.collections;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import java.util.Objects;

/** Validated configuration Object for Lexical (BM-25) indexing configuration for Collections. */
public record CollectionLexicalConfig(
    boolean enabled, @JsonProperty("analyzer") JsonNode analyzerDefinition) {
  public static final String DEFAULT_NAMED_ANALYZER = "standard";

  private static final JsonNode DEFAULT_NAMED_ANALYZER_NODE =
      JsonNodeFactory.instance.textNode(DEFAULT_NAMED_ANALYZER);

  public CollectionLexicalConfig(boolean enabled, JsonNode analyzerDefinition) {
    this.enabled = enabled;
    if (enabled) {
      this.analyzerDefinition = Objects.requireNonNull(analyzerDefinition);
    } else {
      this.analyzerDefinition = analyzerDefinition;
    }
  }

  /**
   * Method for validating the lexical config passed and constructing actual configuration object to
   * use.
   *
   * @return Valid CollectionLexicalConfig object
   */
  public static CollectionLexicalConfig validateAndConstruct(
      ObjectMapper mapper, CreateCollectionCommand.Options.LexicalConfigDefinition lexicalConfig) {
    // If not defined, use default for new collections; valid option
    if (lexicalConfig == null) {
      return configForNewCollections();
    }
    // Otherwise validate and construct
    Boolean enabled = lexicalConfig.enabled();
    if (enabled == null) {
      throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
          "'enabled' is required property for 'lexical' Object value");
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
   * Accessor for an instance to use for a default configuration for newly created collections:
   * where no configuration defined: needs to be enabled, using "standard" analyzer configuration.
   */
  public static CollectionLexicalConfig configForNewCollections() {
    return new CollectionLexicalConfig(true, DEFAULT_NAMED_ANALYZER_NODE);
  }

  /**
   * Accessor for an instance to use for existing pre-lexical collections: ones without lexical
   * field and index: needs to be disabled
   */
  public static CollectionLexicalConfig configForLegacyCollections() {
    return new CollectionLexicalConfig(false, DEFAULT_NAMED_ANALYZER_NODE);
  }

  /**
   * Accessor for an instance to use for missing collection: cases where definition does not exist:
   * needs to be disabled.
   */
  public static CollectionLexicalConfig configForMissingCollection() {
    return new CollectionLexicalConfig(false, DEFAULT_NAMED_ANALYZER_NODE);
  }
}
