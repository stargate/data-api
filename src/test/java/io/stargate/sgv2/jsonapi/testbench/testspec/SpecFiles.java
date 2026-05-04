package io.stargate.sgv2.jsonapi.testbench.testspec;

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

/** Collection of all the {@link io.stargate.sgv2.jsonapi.testbench.testspec.SpecFile} we have loaded
 * from disk.
 * <p>
 * Call {@link #loadAll(List)} to load spec files from multiple directories,
 */
public class SpecFiles {

  private static final ObjectMapper MAPPER =
      new ObjectMapper().configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS, true);

  private final List<SpecFile> specFiles;

  private SpecFiles(List<SpecFile> specFiles) {
    this.specFiles = specFiles;

    for (SpecFile file : specFiles) {
      if (file.spec() instanceof TestSuiteSpec it) {
        // expand the includes
        it.expand(this);
      }
    }
  }

  /**
   * Loads all the spec file, paths is a list of resource dirs in the jar.
   */
  public static SpecFiles loadAll(List<String> paths) {

    var specFiles = resourceDirs(paths).flatMap(SpecFiles::loadAll).toList();
    return new SpecFiles(specFiles);
  }

  /**
   * Get all the SpecFiles by their metadata kind
   */
  public Stream<SpecFile> byKind(TestSpecKind kind) {
    return match(kind, x -> true);
  }

  /**
   * Get all the SpecFiles by the class of the Specification, the object that
   * implements {@link TestSpec}
   */
  public <T extends TestSpec> Stream<T> byType(Class<T> clazz) {
    return match(TestSpecKind.fromType(clazz), x -> true)
            .map(specFile -> specFile.spec().asSpecType(clazz));
  }

  /**
   * Get all the spec files of the type matched by name, e.g. get all the test-suites called "monkey"
   */
  public <T extends TestSpec> Stream<T> byNameAsType(Class<T> clazz, String name) {
    return match(TestSpecKind.fromType(clazz), specFiles -> specFiles.meta().name().equals(name))
        .map(specFile -> specFile.spec().asSpecType(clazz));
  }

  private Stream<SpecFile> match(TestSpecKind kind, Predicate<TestSpec> predicate) {
    return specFiles.stream()
            .filter(itFile -> itFile.spec().meta().kind() == kind)
            .filter(specFile -> predicate.test(specFile.spec()));
  }

  /**
   * Load all the spec files in the directory at the path
   */
  private static Stream<SpecFile> loadAll(Path path) {

    try (Stream<Path> pathStream = Files.walk(path)) {
      return pathStream
          .filter(Files::isRegularFile)
          .filter(SpecFiles::isJsonFile)
          .map(SpecFiles::loadOne)
          .toList() // force so the files are read before closing
          .stream();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed reading test resources under: " + path, e);
    }
  }

  private static boolean isJsonFile(Path file) {
    return file.getFileName().toString().endsWith(".json");
  }


  /**
   * Load a single spec file denoted by path.
   */
  private static SpecFile loadOne(Path path) {
    var file = path.toFile();
    try {
      // It's always JSON
      var root = MAPPER.readTree(file);

      var kindNode = root.path("meta").path("kind");
      if (!kindNode.isTextual()) {
        throw new IllegalArgumentException("Missing/invalid meta.kind in " + file);
      }

      var element =
          switch (TestSpecKind.valueOf(kindNode.asText().toUpperCase())) {
            case ASSERTION_TEMPLATE -> MAPPER.treeToValue(root, AssertionTemplateSpec.class);
            case TARGETS -> MAPPER.treeToValue(root, TargetsSpec.class);
            case TEST_SUITE -> MAPPER.treeToValue(root, TestSuiteSpec.class);
            case WORKFLOW -> MAPPER.treeToValue(root, WorkflowSpec.class);
          };
      return new SpecFile(file, element, root);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed parsing JSON file: " + file, e);
    }
  }


  public static Stream<Path> resourceDirs(List<String> paths) {

    return paths.stream()
            .map(SpecFiles::resourceDir);
  }

  public static Path resourceDir(String path) {

    var cl = Thread.currentThread().getContextClassLoader();
    String normalized = path.startsWith("/") ? path.substring(1) : path;

    var url = cl.getResource(normalized);
    if (url == null) {
      throw new IllegalArgumentException("Test resource folder not found: " + path);
    }

    try {
      // Works for file: URLs; if you run tests from a jar, switch to
      // getResourceAsStream-based
      // walking.
      return Paths.get(url.toURI());
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          "Bad resource URI for: " + path + " -> " + url, e);
    }
  }
}
