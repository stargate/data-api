package io.stargate.sgv2.jsonapi.service.schema.collections;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.schema.versioning.SchemaValue;
import io.stargate.sgv2.jsonapi.util.JsonUtil;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/** Validated configuration Object for Lexical (BM-25) indexing configuration for Collections. */
public record CollectionLexicalDef(
    boolean enabled,
    @JsonInclude(JsonInclude.Include.NON_NULL) @JsonProperty("analyzer")
        JsonNode analyzerDefinition) {

  public static final String DEFAULT_NAMED_ANALYZER = "standard";

  public static final CollectionLexicalDef LEXICAL_DISABLED = new CollectionLexicalDef(false, null);

  private static final JsonNode DEFAULT_NAMED_ANALYZER_NODE =
      JsonNodeFactory.instance.textNode(DEFAULT_NAMED_ANALYZER);

  private static final CollectionLexicalDef DEFAULT_CONFIG =
      new CollectionLexicalDef(true, DEFAULT_NAMED_ANALYZER_NODE);

  private static final CollectionLexicalDef MISSING_CONFIG = new CollectionLexicalDef(false, null);

  // TreeSet just to retain alphabetic order for error message
  private static final Set<String> VALID_ANALYZER_FIELDS =
      new TreeSet<>(Arrays.asList("charFilters", "filters", "tokenizer"));

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
  public CollectionLexicalDef(boolean enabled, JsonNode analyzerDefinition) {
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
              "Analyzer definition should be omitted, JSON null, or an empty JSON object {} if lexical is disabled.");
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
  public static SchemaValue<CollectionLexicalDef> fromApiDesc(
      ObjectMapper mapper,
      CreateCollectionCommand.Options.LexicalDesc lexicalDesc,
      CollectionLexicalDefSchemaFactory lexicalDefSchema) {

    // Case 1: No lexical body provided - so no value from the user
    if (lexicalDesc == null) {
      return lexicalDefSchema.currentVersion(null);
    }

    // Case 2: Validate 'enabled' flag is present
    var enabled = lexicalDesc.enabled();
    if (enabled == null) {
      throw SchemaException.Code.INVALID_CREATE_COLLECTION_OPTIONS.get(
          "message", "'enabled' is required property for 'lexical' Object value");
    }

    // Following cases mean "analyzer" is not defined:
    // 1. No JSON value
    // 2. JSON value itself is null (`null`)
    // 3. JSON value is an empty object (`{}`)
    var analyzerNotDefined =
        (lexicalDesc.analyzerDef() == null)
            || lexicalDesc.analyzerDef().isNull()
            || (lexicalDesc.analyzerDef().isObject() && lexicalDesc.analyzerDef().isEmpty());

    // Case 3: Lexical is disabled - ensure analyzer is absent, JSON null, or empty object {}
    if (!enabled) {
      if (!analyzerNotDefined) {
        String nodeType = JsonUtil.nodeTypeAsString(lexicalDesc.analyzerDef());
        throw SchemaException.Code.INVALID_CREATE_COLLECTION_OPTIONS.get(
            "message",
            ("'lexical' is disabled, but 'lexical.analyzer' property was provided with an unexpected type: %s. "
                    + "When 'lexical' is disabled, 'lexical.analyzer' must either be omitted or be JSON null, or an empty Object '{ }'.")
                .formatted(nodeType));
      }
      // use our clean disabled instance
      return lexicalDefSchema.currentVersion(LEXICAL_DISABLED);
    }

    // Case 5: Enabled and analyzer provided - validate and use
    // Case 5a: missing/null/Empty Object - use default analyzer
    JsonNode cleanedAnalyzerDef;
    if (analyzerNotDefined) {
      // nothing defined, so we use the config which is a string "standard:
      cleanedAnalyzerDef =
          mapper.getNodeFactory().textNode(CollectionLexicalDef.DEFAULT_NAMED_ANALYZER);
    } else if (lexicalDesc.analyzerDef().isTextual()) {
      // Case 5b: JSON String - use as-is -- Could/should we try to validate analyzer name?
      cleanedAnalyzerDef = lexicalDesc.analyzerDef();
    } else if (lexicalDesc.analyzerDef().isObject()) {
      // Case 5c: JSON Object - use as-is but first do light validation
      Set<String> foundNames =
          lexicalDesc.analyzerDef().properties().stream()
              .map(Map.Entry::getKey)
              .collect(Collectors.toSet());

      // First: check top level members for any invalid (misspelled etc) fields
      foundNames.removeAll(VALID_ANALYZER_FIELDS);
      if (!foundNames.isEmpty()) {
        throw SchemaException.Code.INVALID_CREATE_COLLECTION_OPTIONS.get(
            "message",
            "Invalid field%s for 'lexical.analyzer'. Valid fields are: %s, found: %s"
                .formatted(
                    (foundNames.size() == 1 ? "" : "s"),
                    VALID_ANALYZER_FIELDS,
                    new TreeSet<>(foundNames)));
      }

      // Second: check basic data types for allowed fields
      for (Map.Entry<String, JsonNode> entry : lexicalDesc.analyzerDef().properties()) {
        JsonNode fieldValue = entry.getValue();
        // Nulls ok for all
        if (fieldValue.isNull()) {
          continue;
        }
        String expectedType;
        boolean valueOk =
            switch (entry.getKey()) {
              case "tokenizer" -> {
                expectedType = "Object";
                yield fieldValue.isObject();
              }
              default -> {
                expectedType = "Array";
                yield fieldValue.isArray();
              }
            };
        if (!valueOk) {
          throw SchemaException.Code.INVALID_CREATE_COLLECTION_OPTIONS.get(
              "message",
              "'%s' property of 'lexical.analyzer' must be JSON %s, is: %s"
                  .formatted(entry.getKey(), expectedType, JsonUtil.nodeTypeAsString(fieldValue)));
        }
      }

      // all good, use what the user gave us
      cleanedAnalyzerDef = lexicalDesc.analyzerDef();
    } else {
      // Otherwise, invalid definition
      throw SchemaException.Code.INVALID_CREATE_COLLECTION_OPTIONS.get(
          "message",
          "'analyzer' property of 'lexical' must be either JSON Object or String, is: %s"
              .formatted(JsonUtil.nodeTypeAsString(lexicalDesc.analyzerDef())));
    }

    Objects.requireNonNull(cleanedAnalyzerDef, "expected cleanedAnalyzerDef to be non-null");
    return lexicalDefSchema.currentVersion(new CollectionLexicalDef(true, cleanedAnalyzerDef));
  }

  /** Converts this internal lexical representation to the external API representation. */
  public CreateCollectionCommand.Options.LexicalDesc toLexicalDesc() {
    return new CreateCollectionCommand.Options.LexicalDesc(enabled(), analyzerDefinition());
  }

  //  /**
  //   * Accessor for an instance to use for "lexical disabled" Collections (but not for ones
  // pre-dating
  //   * lexical search feature).
  //   */
  //  public static CollectionLexicalDef configForDisabled() {
  //    return new CollectionLexicalDef(false, null);
  //  }

  /**
   * Accessor for a singleton instance used to represent case of default lexical configuration for
   * newly created Collections that do not specify lexical configuration.
   */
  public static CollectionLexicalDef configForDefault() {
    return DEFAULT_CONFIG;
  }

  /**
   * Accessor for a singleton instance used to represent case of missing lexical configuration for
   * legacy Collections created before lexical search was available.
   */
  public static CollectionLexicalDef configForPreLexical() {
    return MISSING_CONFIG;
  }
}
