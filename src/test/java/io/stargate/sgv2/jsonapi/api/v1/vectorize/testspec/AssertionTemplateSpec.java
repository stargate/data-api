package io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.smallrye.health.runtime.SmallRyeIndividualHealthGroupHandler;
import io.stargate.sgv2.jsonapi.api.v1.util.scenarios.ThreeClusteringKeysTableScenario;
import io.stargate.sgv2.jsonapi.service.schema.collections.CqlColumnMatcher;

import java.util.Map;
import java.util.Optional;

public record AssertionTemplateSpec(
    TestSpecMeta meta,
    Map<String, JsonNode> templates
)  implements TestSpec {

  public Optional<JsonNode> templateFor(String name){

    return Optional.ofNullable(templates.get(name));
  }
}
