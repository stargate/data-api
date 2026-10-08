package io.stargate.sgv2.jsonapi.service.reranking.configuration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.smallrye.config.ConfigValidationException;
import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfigBuilder;
import io.smallrye.config.source.yaml.YamlConfigSource;
import io.stargate.sgv2.jsonapi.service.provider.ApiModelSupport;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfig.RerankingProviderConfig.AuthenticationType;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfig.RerankingProviderConfig.ModelConfig;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionRerankDef;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Regression tests for the reranking provider configuration read at startup. The integration test
 * resources add models and providers to the test YAML file with properties, and a mistake there
 * stops the application, so these unit tests build the application's config mapping from that YAML
 * file plus properties. For review: {@link CollectionRerankDef#initializeDefaultRerankDef} accepts
 * one successful call per JVM, so this class only gives it configurations that fail.
 */
class RerankingConfigStartupRegressionTest {

  private static final String YAML_FILE = "/test-reranking-providers-config.yaml";
  private static final String PROVIDERS = "stargate.jsonapi.reranking.providers.";
  private static final String NEW_MODEL = PROVIDERS + "nvidia.models[3].";
  private static final String NEW_PROVIDER = PROVIDERS + "extraProvider.";

  /** The smallest entries that add a model after the three YAML nvidia models, and a provider. */
  private static final Map<String, String> SMALLEST_ENTRIES =
      Map.of(
          NEW_MODEL + "name", "nvidia/unit-test-model",
          NEW_MODEL + "url", "http://localhost/unit-test-model",
          NEW_MODEL + "properties.max-batch-size", "7",
          NEW_PROVIDER + "display-name", "Extra provider",
          NEW_PROVIDER + "enabled", "true");

  // A model with only name, url and max-batch-size is appended after the YAML models and gets
  // defaults for the rest; a provider with only display-name and enabled gets is-default false and
  // no authentications or models; a YAML NONE entry without tokens gets an empty list. The docs do
  // not specify this; the test pins current behavior so that any change is visible in review.
  @Test
  void smallestNewEntriesMapWithDefaults() throws Exception {
    var providers = mapProviders(true, SMALLEST_ENTRIES);
    var models = providers.get("nvidia").models();
    assertThat(models)
        .extracting(ModelConfig::name)
        .containsExactly(
            "nvidia/llama-3.2-nv-rerankqa-1b-v2",
            "nvidia/a-random-deprecated-model",
            "nvidia/a-random-EOL-model",
            "nvidia/unit-test-model");
    var model = models.get(3);
    assertThat(model.url()).isEqualTo("http://localhost/unit-test-model");
    assertThat(model.isDefault()).isFalse();
    assertThat(model.apiModelSupport().status()).isEqualTo(ApiModelSupport.SupportStatus.SUPPORTED);
    assertThat(model.apiModelSupport().message()).isEmpty();
    var properties = model.properties();
    assertThat(properties.atMostRetries()).isEqualTo(3);
    assertThat(properties.initialBackOffMillis()).isEqualTo(100);
    assertThat(properties.maxBackOffMillis()).isEqualTo(500);
    assertThat(properties.readTimeoutMillis()).isEqualTo(5000);
    assertThat(properties.jitter()).isEqualTo(0.5);
    assertThat(properties.maxBatchSize()).isEqualTo(7);
    var extra = providers.get("extraProvider");
    assertThat(extra.isDefault()).isFalse();
    assertThat(extra.supportedAuthentications()).isEmpty();
    assertThat(extra.models()).isEmpty();
    var none = providers.get("nvidia").supportedAuthentications().get(AuthenticationType.NONE);
    assertThat(none.tokens()).isEmpty();
  }

  // Leaving out one of the smallest entries fails the mapping with an error that names the missing
  // property, which stops the application. The docs do not specify this; the test pins current
  // behavior so that any change is visible in review.
  @ParameterizedTest(name = "{0}")
  @MethodSource("requiredProperties")
  void missingRequiredPropertyFailsMapping(String missingProperty) {
    var properties = new HashMap<>(SMALLEST_ENTRIES);
    properties.remove(missingProperty);
    assertThatThrownBy(() -> mapProviders(true, properties))
        .isInstanceOf(ConfigValidationException.class)
        .hasMessageContaining("The config property " + missingProperty + " is required");
  }

  static Stream<Named<String>> requiredProperties() {
    return Stream.of(
        Named.of(
            "new model without properties.max-batch-size -> mapping fails",
            NEW_MODEL + "properties.max-batch-size"),
        Named.of("new model without name -> mapping fails", NEW_MODEL + "name"),
        Named.of("new model without url -> mapping fails", NEW_MODEL + "url"),
        Named.of(
            "new provider without display-name -> mapping fails", NEW_PROVIDER + "display-name"),
        Named.of("new provider without enabled -> mapping fails", NEW_PROVIDER + "enabled"));
  }

  // The startup check that sets the default rerank settings rejects a default model that is not
  // SUPPORTED and a default provider without a NONE authentication entry, which stops the
  // application; when both fail, the model check is reported first. The docs do not specify this;
  // the test pins current behavior so that any change is visible in review.
  @ParameterizedTest(name = "{0}")
  @MethodSource("defaultCheckFailures")
  void startupDefaultCheckRejectsConfig(
      Map<String, String> properties, boolean withYamlFile, String expectedMessage)
      throws Exception {
    var config = new RerankingProvidersConfigImpl(mapProviders(withYamlFile, properties));
    assertThatThrownBy(() -> CollectionRerankDef.initializeDefaultRerankDef(config))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(expectedMessage);
  }

  static Stream<Arguments> defaultCheckFailures() {
    var status = PROVIDERS + "nvidia.models[0].api-model-support.status";
    var noSupportedModel = "provider 'nvidia' does not have a default supported model";
    var nvidia = PROVIDERS + "nvidia.";
    // Properties cannot remove NONE from the YAML map, so this config replaces the whole file.
    var nvidiaWithoutNone =
        Map.of(
            nvidia + "is-default", "true",
            nvidia + "display-name", "Nvidia",
            nvidia + "enabled", "true",
            nvidia + "supported-authentications.HEADER.enabled", "true",
            nvidia + "models[0].name", "nvidia/llama-3.2-nv-rerankqa-1b-v2",
            nvidia + "models[0].is-default", "true",
            nvidia + "models[0].url", "http://localhost/unit-test-model",
            nvidia + "models[0].properties.max-batch-size", "10");
    var otherModelSupported = new HashMap<>(SMALLEST_ENTRIES);
    otherModelSupported.put(status, "DEPRECATED");
    var deprecatedWithoutNone = new HashMap<>(nvidiaWithoutNone);
    deprecatedWithoutNone.put(status, "DEPRECATED");
    return Stream.of(
        Arguments.of(
            Named.of("default model DEPRECATED -> rejected", Map.of(status, "DEPRECATED")),
            true,
            noSupportedModel),
        Arguments.of(
            Named.of("default model END_OF_LIFE -> rejected", Map.of(status, "END_OF_LIFE")),
            true,
            noSupportedModel),
        // A SUPPORTED nvidia model that is not is-default does not stand in for the default model.
        Arguments.of(
            Named.of(
                "default model DEPRECATED, other nvidia model SUPPORTED -> rejected",
                otherModelSupported),
            true,
            noSupportedModel),
        Arguments.of(
            Named.of("default provider without NONE authentication -> rejected", nvidiaWithoutNone),
            false,
            "provider 'nvidia' does not support 'NONE' authentication type"),
        Arguments.of(
            Named.of(
                "default model DEPRECATED and no NONE authentication -> model check reported first",
                deprecatedWithoutNone),
            false,
            noSupportedModel));
  }

  /** Maps the providers like the application: the test YAML file if asked, properties on top. */
  private static Map<String, RerankingProvidersConfig.RerankingProviderConfig> mapProviders(
      boolean withYamlFile, Map<String, String> properties) throws Exception {
    var builder =
        new SmallRyeConfigBuilder()
            .withMapping(DefaultRerankingProviderConfig.class)
            // 400 is the priority of system properties, above the YAML file.
            .withSources(new PropertiesConfigSource(properties, "test-properties", 400));
    if (withYamlFile) {
      var yaml = RerankingConfigStartupRegressionTest.class.getResource(YAML_FILE);
      builder.withSources(new YamlConfigSource(yaml));
    }
    return builder.build().getConfigMapping(DefaultRerankingProviderConfig.class).providers();
  }
}
