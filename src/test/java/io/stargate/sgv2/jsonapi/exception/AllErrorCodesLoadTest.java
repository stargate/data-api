package io.stargate.sgv2.jsonapi.exception;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Systemic guard that every error {@link ErrorCode} loads its {@link ErrorTemplate} from the real
 * {@code errors.yaml} and has the expected content.
 *
 * <p>Follow-up to <a href="https://github.com/stargate/data-api/issues/2482">#2482</a> (out of
 * #2134): templates are loaded in each enum's static initializer, so a {@code Code} that no other
 * test exercises can hide a broken {@code errors.yaml} entry (e.g. a {@code scope:} that does not
 * match the exception's {@code SCOPE}, which is what #2134 was). Listing every {@code Code} here
 * forces those loads. Adding a new exception means adding its {@code Code} to {@link
 * #allErrorCodes()}.
 */
public class AllErrorCodesLoadTest {

  /** Every {@link ErrorCode} enum; referencing {@code values()} forces the template load. */
  static Stream<ErrorCode<?>> allErrorCodes() {
    return Stream.of(
            APISecurityException.Code.values(),
            DatabaseException.Code.values(),
            DocumentException.Code.values(),
            EmbeddingProviderException.Code.values(),
            FilterException.Code.values(),
            ProjectionException.Code.values(),
            RequestException.Code.values(),
            RerankingProviderException.Code.values(),
            SchemaException.Code.values(),
            ServerException.Code.values(),
            SortException.Code.values(),
            UpdateException.Code.values(),
            WarningException.Code.values())
        .flatMap(Arrays::stream);
  }

  @ParameterizedTest
  @MethodSource("allErrorCodes")
  public void loadsTemplateFromErrorsYaml(ErrorCode<?> code) {
    var template = code.template();

    assertThat(template).as("template for %s", code.name()).isNotNull();
    assertThat(template.code()).as("template code matches enum name").isEqualTo(code.name());
    assertThat(template.title()).as("title for %s", code.name()).isNotBlank();
    assertThat(template.messageTemplate()).as("message body for %s", code.name()).isNotBlank();
  }
}
