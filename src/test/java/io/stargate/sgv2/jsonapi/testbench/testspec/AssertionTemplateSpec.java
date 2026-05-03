package io.stargate.sgv2.jsonapi.testbench.testspec;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import java.util.Optional;

public record AssertionTemplateSpec(TestSpecMeta meta, Map<String, JsonNode> templates)
    implements TestSpec {

  public Optional<JsonNode> templateFor(String name) {

    return Optional.ofNullable(templates.get(name));
  }
}
