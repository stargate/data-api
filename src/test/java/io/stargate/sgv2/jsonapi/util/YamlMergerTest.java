package io.stargate.sgv2.jsonapi.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class YamlMergerTest {

  private YamlMerger merger;
  private ObjectMapper yamlMapper;

  @BeforeEach
  void setUp() {
    merger = new YamlMerger();
    yamlMapper = new ObjectMapper(new YAMLFactory());
  }

  private String loadResource(String absolutePath) {
    try {
      return Files.readString(Path.of(absolutePath));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void example_scalar_and_object_merge_semantics() throws Exception {
    String base =
        """
            server:
              host: localhost
              port: 8080
              ssl:
                enabled: false
                protocols: [TLSv1.2]
            """;
    String patch =
        """
            server:
              port: 9090
              ssl:
                enabled: true
            """;
    String output =
        """
            server:
              host: localhost
              port: 9090
              ssl:
                enabled: true
                protocols: [TLSv1.2]
            """;

    String mergedOutput = merger.mergeYamlStrings(base, patch);
    JsonNode expectedNode = yamlMapper.readTree(output);
    JsonNode actualNode = yamlMapper.readTree(mergedOutput);
    assertThat(actualNode).isEqualTo(expectedNode);
  }

  // Embedding providers config path
  private static final String CONFIG_PATH = "src/main/resources/embedding-providers-config.yaml";

  @Test
  void patch_enable_disable_openai_provider() throws Exception {
    String base = loadResource(CONFIG_PATH);

    String patchDisable =
        """
            stargate:
              jsonapi:
                embedding:
                  providers:
                    openai:
                      enabled: false
            """;

    String outDisabled = merger.mergeYamlStrings(base, patchDisable);
    JsonNode disabled = yamlMapper.readTree(outDisabled);
    assertThat(disabled.at("/stargate/jsonapi/embedding/providers/openai/enabled").asBoolean())
        .isFalse();

    String patchEnable =
        """
            stargate:
              jsonapi:
                embedding:
                  providers:
                    openai:
                      enabled: true
            """;

    String outEnabled = merger.mergeYamlStrings(base, patchEnable);
    JsonNode enabled = yamlMapper.readTree(outEnabled);
    assertThat(enabled.at("/stargate/jsonapi/embedding/providers/openai/enabled").asBoolean())
        .isTrue();
  }

  @Test
  void patch_enable_nvidia_set_url_and_replace_models() throws Exception {
    String base = loadResource(CONFIG_PATH);
    String patch =
        """
            stargate:
              jsonapi:
                embedding:
                  providers:
                    nvidia:
                      enabled: true
                      url: https://new.nvidia.example/embeddings
                      models:
                        - name: nv-new-model-a
                          vector-dimension: 2048
                        - name: nv-new-model-b
                          vector-dimension: 512
            """;

    String out = merger.mergeYamlStrings(base, patch);
    JsonNode node = yamlMapper.readTree(out);

    // Verify the enabled and url
    assertThat(node.at("/stargate/jsonapi/embedding/providers/nvidia/enabled").asBoolean())
        .isTrue();
    assertThat(node.at("/stargate/jsonapi/embedding/providers/nvidia/url").asText())
        .isEqualTo("https://new.nvidia.example/embeddings");

    // Verify the model list is replaced entirely
    assertThat(node.at("/stargate/jsonapi/embedding/providers/nvidia/models").isArray()).isTrue();
    assertThat(node.at("/stargate/jsonapi/embedding/providers/nvidia/models").size()).isEqualTo(2);
    assertThat(node.at("/stargate/jsonapi/embedding/providers/nvidia/models/0/name").asText())
        .isEqualTo("nv-new-model-a");
    assertThat(
            node.at("/stargate/jsonapi/embedding/providers/nvidia/models/0/vector-dimension")
                .asInt())
        .isEqualTo(2048);
    assertThat(node.at("/stargate/jsonapi/embedding/providers/nvidia/models/1/name").asText())
        .isEqualTo("nv-new-model-b");
    assertThat(
            node.at("/stargate/jsonapi/embedding/providers/nvidia/models/1/vector-dimension")
                .asInt())
        .isEqualTo(512);
  }

  @Test
  void patch_enable_openai_set_url_and_replace_models() throws Exception {
    String base = loadResource(CONFIG_PATH);
    String patch =
        """
            stargate:
              jsonapi:
                embedding:
                  providers:
                    openai:
                      enabled: true
                      url: https://api.openai.com/v2/
                      models:
                        - name: new-embed-small
                          parameters:
                            - name: vectorDimension
                              type: number
                              required: true
                              default-value: 256
                        - name: new-embed-large
                          parameters:
                            - name: vectorDimension
                              type: number
                              required: true
                              default-value: 4096
            """;

    String out = merger.mergeYamlStrings(base, patch);
    JsonNode node = yamlMapper.readTree(out);

    // Verify the enabled and url
    assertThat(node.at("/stargate/jsonapi/embedding/providers/openai/enabled").asBoolean())
        .isTrue();
    assertThat(node.at("/stargate/jsonapi/embedding/providers/openai/url").asText())
        .isEqualTo("https://api.openai.com/v2/");

    // Verify the model list is replaced entirely
    assertThat(node.at("/stargate/jsonapi/embedding/providers/openai/models").isArray()).isTrue();
    assertThat(node.at("/stargate/jsonapi/embedding/providers/openai/models").size()).isEqualTo(2);
    assertThat(node.at("/stargate/jsonapi/embedding/providers/openai/models/0/name").asText())
        .isEqualTo("new-embed-small");
    assertThat(
            node.at(
                    "/stargate/jsonapi/embedding/providers/openai/models/0/parameters/0/default-value")
                .asInt())
        .isEqualTo(256);

    assertThat(node.at("/stargate/jsonapi/embedding/providers/openai/models/1/name").asText())
        .isEqualTo("new-embed-large");
    assertThat(
            node.at(
                    "/stargate/jsonapi/embedding/providers/openai/models/1/parameters/0/default-value")
                .asInt())
        .isEqualTo(4096);
  }

  @Test
  void patch_replace_nvidia_models_with_empty_list() throws Exception {
    String base = loadResource(CONFIG_PATH);
    String patch =
        """
            stargate:
              jsonapi:
                embedding:
                  providers:
                    nvidia:
                      models: []
            """;

    String out = merger.mergeYamlStrings(base, patch);
    JsonNode node = yamlMapper.readTree(out);
    assertThat(node.at("/stargate/jsonapi/embedding/providers/nvidia/models").isArray()).isTrue();
    assertThat(node.at("/stargate/jsonapi/embedding/providers/nvidia/models").size()).isEqualTo(0);
  }

  @Test
  void patch_openai_set_url_null_removes_field() throws Exception {
    String base = loadResource(CONFIG_PATH);
    String patch =
        """
            stargate:
              jsonapi:
                embedding:
                  providers:
                    openai:
                      url: null
            """;

    String out = merger.mergeYamlStrings(base, patch);
    JsonNode node = yamlMapper.readTree(out);
    assertThat(node.at("/stargate/jsonapi/embedding/providers/openai/url").isNull()).isTrue();
  }

  @Test
  void patch_openai_models_object_replaces_array() throws Exception {
    String base = loadResource(CONFIG_PATH);
    String patch =
        """
            stargate:
              jsonapi:
                embedding:
                  providers:
                    openai:
                      models: { replaced: true }
            """;

    String out = merger.mergeYamlStrings(base, patch);
    JsonNode node = yamlMapper.readTree(out);
    assertThat(node.at("/stargate/jsonapi/embedding/providers/openai/models").isObject()).isTrue();
    assertThat(node.at("/stargate/jsonapi/embedding/providers/openai/models/replaced").asBoolean())
        .isTrue();
  }

  @Test
  void patch_add_new_provider_custom() throws Exception {
    String base = loadResource(CONFIG_PATH);
    String patch =
        """
            stargate:
              jsonapi:
                embedding:
                  providers:
                    customAI:
                      display-name: Custom AI
                      enabled: true
                      url: https://custom.ai/v1/embeddings
                      models:
                        - name: custom-embed-a
                          vector-dimension: 128
            """;

    String out = merger.mergeYamlStrings(base, patch);
    JsonNode node = yamlMapper.readTree(out);
    assertThat(node.at("/stargate/jsonapi/embedding/providers/customAI/enabled").asBoolean())
        .isTrue();
    assertThat(node.at("/stargate/jsonapi/embedding/providers/customAI/url").asText())
        .isEqualTo("https://custom.ai/v1/embeddings");
    assertThat(node.at("/stargate/jsonapi/embedding/providers/customAI/models/0/name").asText())
        .isEqualTo("custom-embed-a");
  }

  @Test
  void patch_merges_nested_object_fields_preserving_siblings() throws Exception {
    String base = loadResource(CONFIG_PATH);
    // Flip HEADER.enabled under openai supported-authentications, but preserve HEADER.tokens
    String patch =
        """
            stargate:
              jsonapi:
                embedding:
                  providers:
                    openai:
                      supported-authentications:
                        HEADER:
                          enabled: false
            """;

    String out = merger.mergeYamlStrings(base, patch);
    JsonNode node = yamlMapper.readTree(out);

    // The supported-authentications size should not change
    assertThat(
            node.at("/stargate/jsonapi/embedding/providers/openai/supported-authentications")
                .size())
        .isEqualTo(3);

    assertThat(
            node.at(
                    "/stargate/jsonapi/embedding/providers/openai/supported-authentications/HEADER/enabled")
                .asBoolean())
        .isFalse();
    // Tokens array should still be present (object merge retains siblings)
    assertThat(
            node.at(
                    "/stargate/jsonapi/embedding/providers/openai/supported-authentications/HEADER/tokens")
                .isArray())
        .isTrue();
    assertThat(
            node.at(
                    "/stargate/jsonapi/embedding/providers/openai/supported-authentications/HEADER/tokens/0/accepted")
                .asText())
        .isNotEmpty();
  }

  @Test
  void merge_yaml_streams_equivalence() throws Exception {
    String base =
        """
            a:
              b: 1
              c:
                d: true
            """;
    String patch =
        """
            a:
              b: 2
              e: test
            """;

    String mergedFromStrings = merger.mergeYamlStrings(base, patch);

    try (ByteArrayInputStream baseIn =
            new ByteArrayInputStream(base.getBytes(StandardCharsets.UTF_8));
        ByteArrayInputStream patchIn =
            new ByteArrayInputStream(patch.getBytes(StandardCharsets.UTF_8))) {
      String mergedFromStreams = merger.mergeYamlStreams(baseIn, patchIn);
      JsonNode n1 = yamlMapper.readTree(mergedFromStrings);
      JsonNode n2 = yamlMapper.readTree(mergedFromStreams);
      assertThat(n1).isEqualTo(n2);
      // Also verify expected values
      assertThat(n1.at("/a/b").asInt()).isEqualTo(2);
      assertThat(n1.at("/a/c/d").asBoolean()).isTrue();
      assertThat(n1.at("/a/e").asText()).isEqualTo("test");
    }
  }

  @Test
  void mergeYamlStrings_invalidYaml_throws() {
    String invalidBase = ": not-yaml";
    String validPatch = "a: 1";
    assertThatThrownBy(() -> merger.mergeYamlStrings(invalidBase, validPatch))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Failed to merge YAML");
  }

  @Test
  void mergeYamlStreams_invalidYaml_throws() {
    byte[] invalid = ": not-yaml".getBytes(StandardCharsets.UTF_8);
    byte[] valid = "a: 1".getBytes(StandardCharsets.UTF_8);
    try (ByteArrayInputStream baseIn = new ByteArrayInputStream(invalid);
        ByteArrayInputStream patchIn = new ByteArrayInputStream(valid)) {
      assertThatThrownBy(() -> merger.mergeYamlStreams(baseIn, patchIn))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Failed to merge YAML streams");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void mergeNodes_baseObject_patchNull_returnsBaseDeepCopy() throws Exception {
    JsonNode base = yamlMapper.readTree("a: 1\nb: 2\n");
    JsonNode result = merger.mergeNodes(base, null);
    assertThat(result).isNotSameAs(base);
    assertThat(result.at("/a").asInt()).isEqualTo(1);
    assertThat(result.at("/b").asInt()).isEqualTo(2);
  }

  @Test
  void mergeNodes_baseNull_patchObject_returnsPatchDeepCopy() throws Exception {
    JsonNode patch = yamlMapper.readTree("a: 3\nc: test\n");
    JsonNode result = merger.mergeNodes(null, patch);
    assertThat(result).isNotSameAs(patch);
    assertThat(result.at("/a").asInt()).isEqualTo(3);
    assertThat(result.at("/c").asText()).isEqualTo("test");
  }

  @Test
  void mergeNodes_bothNull_returnsNull() {
    JsonNode result = merger.mergeNodes(null, null);
    assertThat(result).isNull();
  }
}
