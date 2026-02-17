package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class ITCollection {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final Configuration config =
      Configuration.builder()
          .jsonProvider(new JacksonJsonNodeJsonProvider())
          .mappingProvider(new JacksonMappingProvider())
          .build();

  List<ITFile> itFiles;

  private ITCollection(List<ITFile> itFiles) {
    this.itFiles = itFiles;
    for (ITFile file : itFiles) {
     if (file.element() instanceof IntegrationTest it){
       it.expand(this);
     }
    }
  }

  public IntegrationWorkflow workflowFirstByName(String name) {
    var path = "$.meta[?(@.name == '%s')]".formatted(name);

    var itFile =
        match(ITElement.ITElementKind.WORKFLOW, path).stream()
            .findFirst()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "No IT file found with meta.name == '%s'".formatted(name)));
    return (IntegrationWorkflow) itFile.element();
  }

  public IntegrationTest testsFirstByName(String name) {
    return testsByName(name).getFirst();
  }

  public List<IntegrationTest> testsByName(String name) {
    var path = "$.meta[?(@.name == '%s')]".formatted(name);

    return match(ITElement.ITElementKind.TEST, path).stream()
        .map(itFile -> (IntegrationTest) itFile.element())
        .toList();
  }

  private List<ITFile> byKind(ITElement.ITElementKind kind) {
    return itFiles.stream().filter(itFile -> itFile.element().kind() == kind).toList();
  }

  private List<ITFile> match(ITElement.ITElementKind kind, String jsonPath) {
    var compiled = JsonPath.compile(jsonPath);

    return byKind(kind).stream().filter(itFile -> hasMatch(itFile.root(), compiled)).toList();
  }

  private boolean hasMatch(JsonNode root, JsonPath compiled) {
    var pathResult = JsonPath.using(config).parse(root).read(compiled);

    return switch (pathResult) {
      case null -> false;
      case java.util.Collection<?> c -> !c.isEmpty();
      case java.util.Map<?, ?> m -> !m.isEmpty();
      case ArrayNode a -> !a.isEmpty();
      case ObjectNode o -> !o.isEmpty();
      default -> true;
    };
  }

  static ITCollection loadAll(String path) {
    final Path dir = resourceDir(path);

    List<ITFile> itFiles = new ArrayList<>();

    try (Stream<Path> s = Files.walk(dir)) {
      itFiles =
          s.filter(Files::isRegularFile)
              .filter(p -> p.getFileName().toString().endsWith(".json"))
              .map(ITCollection::loadOne)
              .toList();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed reading test resources under: " + dir, e);
    }
    return new ITCollection(itFiles);
  }

  private static ITFile loadOne(Path file) {
    try {
      var root = MAPPER.readTree(file.toFile());

      JsonNode kindNode = root.path("meta").path("kind");
      if (!kindNode.isTextual()) {
        throw new IllegalArgumentException("Missing/invalid meta.kind in " + file);
      }

      var kind = kindNode.asText();
      var element =
          switch (kind.toUpperCase()) {
            case "TEST" -> MAPPER.treeToValue(root, IntegrationTest.class);
            case "WORKFLOW" -> MAPPER.treeToValue(root, IntegrationWorkflow.class);
            default ->
                throw new IllegalArgumentException("Unknown meta.kind '" + kind + "' in " + file);
          };
      return new ITFile(element, root);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed parsing JSON file: " + file, e);
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
