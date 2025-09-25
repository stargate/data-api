package io.stargate.sgv2.jsonapi.util;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
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
        "server:\n  host: localhost\n  port: 8080\n  ssl:\n    enabled: false\n    protocols: [TLSv1.2]\n";
    String patch = "server:\n  port: 9090\n  ssl:\n    enabled: true\n";

    String out = merger.mergeYamlStrings(base, patch);
    JsonNode node = yamlMapper.readTree(out);

    assertThat(node.at("/server/host").asText()).isEqualTo("localhost");
    assertThat(node.at("/server/port").asInt()).isEqualTo(9090);
    assertThat(node.at("/server/ssl/enabled").asBoolean()).isTrue();
    assertThat(node.at("/server/ssl/protocols/0").asText()).isEqualTo("TLSv1.2");
  }

  // Embedding providers config path
  private static final String CONFIG_PATH =
      "/Users/hazel.he/Desktop/jsonapi/src/main/resources/embedding-providers-config.yaml";

  @Test
  void patch_enable_disable_openai_provider() throws Exception {
    String base = loadResource(CONFIG_PATH);

    String patchDisable =
        "stargate:\n  jsonapi:\n    embedding:\n      providers:\n        openai:\n          enabled: false\n";

    String outDisabled = merger.mergeYamlStrings(base, patchDisable);
    JsonNode disabled = yamlMapper.readTree(outDisabled);
    assertThat(disabled.at("/stargate/jsonapi/embedding/providers/openai/enabled").asBoolean())
        .isFalse();

    String patchEnable =
        "stargate:\n  jsonapi:\n    embedding:\n      providers:\n        openai:\n          enabled: true\n";

    String outEnabled = merger.mergeYamlStrings(base, patchEnable);
    JsonNode enabled = yamlMapper.readTree(outEnabled);
    assertThat(enabled.at("/stargate/jsonapi/embedding/providers/openai/enabled").asBoolean())
        .isTrue();
  }

  @Test
  void patch_change_nvidia_url_and_enable() throws Exception {
    String base = loadResource(CONFIG_PATH);
    String patch =
        "stargate:\n  jsonapi:\n    embedding:\n      providers:\n        nvidia:\n          enabled: true\n          url: https://new.nvidia.example/embeddings\n";

    String out = merger.mergeYamlStrings(base, patch);
    JsonNode node = yamlMapper.readTree(out);
    assertThat(node.at("/stargate/jsonapi/embedding/providers/nvidia/enabled").asBoolean())
        .isTrue();
    assertThat(node.at("/stargate/jsonapi/embedding/providers/nvidia/url").asText())
        .isEqualTo("https://new.nvidia.example/embeddings");
  }

  @Test
  void patch_enable_nvidia_set_url_and_replace_models() throws Exception {
    String base = loadResource(CONFIG_PATH);
    String patch =
        "stargate:\n  jsonapi:\n    embedding:\n      providers:\n        nvidia:\n          enabled: true\n          url: https://new.nvidia.example/embeddings\n          models:\n            - name: nv-new-model-a\n              vector-dimension: 2048\n            - name: nv-new-model-b\n              vector-dimension: 512\n";

    String out = merger.mergeYamlStrings(base, patch);
    JsonNode node = yamlMapper.readTree(out);

    assertThat(node.at("/stargate/jsonapi/embedding/providers/nvidia/enabled").asBoolean())
        .isTrue();
    assertThat(node.at("/stargate/jsonapi/embedding/providers/nvidia/url").asText())
        .isEqualTo("https://new.nvidia.example/embeddings");

    // Array replacement semantics
    assertThat(node.at("/stargate/jsonapi/embedding/providers/nvidia/models").isArray()).isTrue();
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
        "stargate:\n  jsonapi:\n    embedding:\n      providers:\n        openai:\n          enabled: true\n          url: https://api.openai.com/v2/\n          models:\n            - name: new-embed-small\n              parameters:\n                - name: vectorDimension\n                  type: number\n                  required: true\n                  default-value: 256\n            - name: new-embed-large\n              parameters:\n                - name: vectorDimension\n                  type: number\n                  required: true\n                  default-value: 4096\n";

    String out = merger.mergeYamlStrings(base, patch);
    JsonNode node = yamlMapper.readTree(out);

    assertThat(node.at("/stargate/jsonapi/embedding/providers/openai/enabled").asBoolean())
        .isTrue();
    assertThat(node.at("/stargate/jsonapi/embedding/providers/openai/url").asText())
        .isEqualTo("https://api.openai.com/v2/");

    assertThat(node.at("/stargate/jsonapi/embedding/providers/openai/models").isArray()).isTrue();
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
}
