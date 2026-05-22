package io.stargate.sgv2.jsonapi.exception;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Systemic guard that every error {@link ErrorCode} enum can be loaded against the real {@code
 * errors.yaml}, forcing the {@link ErrorTemplate} load and validating the loaded fields.
 *
 * <p>Follow-up to <a href="https://github.com/stargate/data-api/issues/2482">#2482</a> (which came
 * out of #2134): the templates are loaded lazily in each enum's static initializer, so a {@code
 * Code} that is never exercised by another test can hide a broken/misaligned {@code errors.yaml}
 * entry (e.g. a {@code scope:} that does not match the exception's {@code SCOPE}). #2134 was
 * exactly such a latent bug.
 *
 * <p>The {@code Code} enums are discovered from the compiled classes rather than hard-coded, so a
 * newly added exception class is covered automatically.
 */
public class AllErrorCodesLoadTest {

  private static final String EXCEPTION_PACKAGE = APIException.class.getPackageName();
  private static final String CODE_CLASS_SUFFIX = "$Code.class";

  /**
   * Discovers all {@code *Exception$Code} enum classes in the exception package by listing the
   * compiled class files. Classes are loaded without initialization so the static {@link
   * ErrorTemplate} load is deferred to (and asserted by) the test body.
   */
  static Stream<Class<?>> errorCodeEnums() throws Exception {
    // Resolve the package directory from a class that is guaranteed to live in it.
    URL anchor = APIException.class.getResource("APIException.class");
    assertThat(anchor)
        .as("exception package must be on the filesystem classpath for discovery")
        .isNotNull();
    Path packageDir = Path.of(anchor.toURI()).getParent();

    ClassLoader loader = AllErrorCodesLoadTest.class.getClassLoader();
    List<Class<?>> codeEnums = new ArrayList<>();
    try (var files = Files.list(packageDir)) {
      for (Path file : (Iterable<Path>) files::iterator) {
        String fileName = file.getFileName().toString();
        if (!fileName.endsWith(CODE_CLASS_SUFFIX)) {
          continue;
        }
        String simpleName = fileName.substring(0, fileName.length() - ".class".length());
        // load without initializing: defer the errors.yaml template load to the test body
        codeEnums.add(Class.forName(EXCEPTION_PACKAGE + "." + simpleName, false, loader));
      }
    }
    return codeEnums.stream();
  }

  @Test
  public void discoveryFindsKnownCodeEnums() throws Exception {
    var discovered = errorCodeEnums().toList();

    // Sanity check the scanner actually found the enums, so the parameterized test below can not
    // silently pass with an empty set if discovery ever breaks.
    assertThat(discovered)
        .as("discovered error Code enums")
        .hasSizeGreaterThanOrEqualTo(13)
        .contains(
            RerankingProviderException.Code.class,
            EmbeddingProviderException.Code.class,
            FilterException.Code.class,
            ServerException.Code.class);
  }

  @ParameterizedTest
  @MethodSource("errorCodeEnums")
  public void codeEnumLoadsAndProcessesTemplates(Class<?> codeEnumClass) {

    assertThat(codeEnumClass)
        .as("%s should be an enum implementing ErrorCode", codeEnumClass.getName())
        .matches(Class::isEnum, "isEnum")
        .matches(ErrorCode.class::isAssignableFrom, "implements ErrorCode");

    // Forcing initialization runs the static block that calls ErrorTemplate.load() for every
    // constant; this throws (ExceptionInInitializerError) if any errors.yaml entry is missing or
    // mismatched, which is the #2134 class of bug we want to catch here.
    assertDoesNotThrow(
        () -> Class.forName(codeEnumClass.getName(), true, codeEnumClass.getClassLoader()),
        "errors.yaml template load must succeed for " + codeEnumClass.getName());

    Object[] constants = codeEnumClass.getEnumConstants();
    assertThat(constants)
        .as("%s should declare at least one error code", codeEnumClass.getName())
        .isNotEmpty();

    for (Object constant : constants) {
      var code = (ErrorCode<?>) constant;
      var template = code.template();

      assertThat(template)
          .as("template for %s.%s", codeEnumClass.getSimpleName(), code.name())
          .isNotNull();
      assertThat(template.code())
          .as("template code matches enum name for %s", code.name())
          .isEqualTo(code.name());
      assertThat(template.family()).as("family for %s", code.name()).isNotNull();
      assertThat(template.scope()).as("scope for %s", code.name()).isNotNull();
      assertThat(template.title()).as("title for %s", code.name()).isNotBlank();
      assertThat(template.messageTemplate()).as("message body for %s", code.name()).isNotBlank();
    }
  }
}
