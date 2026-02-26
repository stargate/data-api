package io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.TargetConfigurationss;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.TestSpecMeta;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

public record AssertionTemplates(
    TestSpecMeta meta,
    ObjectNode templates
) {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static AssertionTemplates load(){

    final Path dir = resourceDir("integration-tests/assertions/assertion-templates.json");

    try {
      return MAPPER.readValue(dir.toFile(), AssertionTemplates.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static Path resourceDir(String path) {
    String normalized = path.startsWith("/") ? path.substring(1) : path;

    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    URL url = cl.getResource(normalized);
    if (url == null) {
      throw new IllegalArgumentException("Test resource folder not found: " + path);
    }

    try {
      // Works for file: URLs; if you run tests from a jar, switch to getResourceAsStream-based
      // walking.
      return Paths.get(url.toURI());
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Bad resource URI for: " + path + " -> " + url, e);
    }
  }
}
