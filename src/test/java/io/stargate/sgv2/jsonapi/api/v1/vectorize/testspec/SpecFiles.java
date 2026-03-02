package io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Collection of all the test spec files read for this execution.
 */
public class SpecFiles {

  private static final ObjectMapper MAPPER = new ObjectMapper()
      .configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS, true);

  private final List<SpecFile> specFiles;

  private SpecFiles(List<SpecFile> specFiles) {
    this.specFiles = specFiles;

    for (SpecFile file : specFiles) {
     if (file.spec() instanceof TestSuite it){
       it.expand(this);
     }
    }
  }

  public static SpecFiles loadAll(List<String> paths) {

    var specFiles = resourceDirs(paths)
        .flatMap(SpecFiles::loadAll)
        .toList();
    return new SpecFiles(specFiles);
  }

  public Stream<SpecFile> byKind(TestSpecKind kind) {
    return specFiles.stream()
        .filter(itFile -> itFile.spec().meta().kind() == kind);
  }

  public <T extends TestSpec> Stream<T> byType(Class<T> clazz) {
    return byKind(TestSpecKind.fromType(clazz))
        .map(specFile -> specFile.spec().asSpecType(clazz));
  }

  public Stream<SpecFile> byName(TestSpecKind kind, String name) {
    return match(kind, specFiles -> specFiles.meta().name().equals(name));
  }

  public <T extends TestSpec> Stream<T> byNameAsType(Class<T> clazz, String name) {
    return match(TestSpecKind.fromType(clazz), specFiles -> specFiles.meta().name().equals(name))
        .map(specFile -> specFile.spec().asSpecType(clazz));
  }


  private Stream<SpecFile> match(TestSpecKind kind, Predicate<TestSpec> predicate) {
    return byKind(kind)
        .filter(specFile -> predicate.test(specFile.spec()));
  }

  private static Stream<SpecFile> loadAll(Path path) {

    try (Stream<Path> pathStream = Files.walk(path)) {
      return pathStream.filter(Files::isRegularFile)
              .filter(SpecFiles::isJsonFile)
              .map(SpecFiles::loadOne)
              .toList() // force so the files are read before closing
              .stream();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed reading test resources under: " + path , e);
    }
  }

  private static SpecFile loadOne(Path path) {
    var file = path.toFile();
    try {
      var root = MAPPER.readTree(file);

      var kindNode = root.path("meta").path("kind");
      if (!kindNode.isTextual()) {
        throw new IllegalArgumentException("Missing/invalid meta.kind in " + file);
      }

      var element =
          switch (TestSpecKind.valueOf(kindNode.asText().toUpperCase())) {
            case ASSERTION_TEMPLATE -> MAPPER.treeToValue(root, AssertionTemplateSpec.class);
            case TEST_SUITE -> MAPPER.treeToValue(root, TestSuite.class);
            case WORKFLOW -> MAPPER.treeToValue(root, Workflow.class);

          };
      return new SpecFile(file, element, root);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed parsing JSON file: " + file, e);
    }
  }

  private static boolean isJsonFile(Path file) {
    return file.getFileName().toString().endsWith(".json");
  }

  private static Stream<Path> resourceDirs(List<String> paths) {

    var cl = Thread.currentThread().getContextClassLoader();

    return paths.stream().map(
        path  -> {
          String normalized = path.startsWith("/") ? path.substring(1) : path;

          var url = cl.getResource(normalized);
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
    );
  }
}
