package io.stargate.sgv2.jsonapi.api.v1.findandrerank;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsStatusOnly;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankAssertions.assertApiError;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankAssertions.assertIds;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.BYO;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.COMMENT;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.EXAMPLE_VECTOR;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.MAIN;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.NORERANK;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.passages;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankRequests.findAndRerank;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.RerankerAssertions.assertRerankerCalls;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.RerankerAssertions.assertRerankerNotCalled;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.RerankerAssertions.calls;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.RerankerAssertions.defaultModelCalls;
import static io.stargate.sgv2.jsonapi.exception.RequestException.Code.COMMAND_FIELD_UNKNOWN;
import static io.stargate.sgv2.jsonapi.exception.RequestException.Code.INVALID_RERANK_OVERRIDE;
import static io.stargate.sgv2.jsonapi.exception.RequestException.Code.REQUEST_STRUCTURE_MISMATCH;
import static io.stargate.sgv2.jsonapi.exception.RequestException.Code.UNSUPPORTED_RERANKING_COMMAND;
import static io.stargate.sgv2.jsonapi.exception.SchemaException.Code.DEPRECATED_AI_MODEL;
import static io.stargate.sgv2.jsonapi.exception.SchemaException.Code.END_OF_LIFE_AI_MODEL;
import static io.stargate.sgv2.jsonapi.exception.SchemaException.Code.RERANKING_SERVICE_TYPE_UNAVAILABLE;
import static io.stargate.sgv2.jsonapi.exception.ServerException.Code.UNEXPECTED_SERVER_ERROR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.restassured.response.ValidatableResponse;
import io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.Fixture;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.testresource.FakeRerankerTestResource.Model;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * findAndRerank tests of the {@code rerank} option, which replaces the collection's reranker for
 * one request, and of collections whose stored reranker model is deprecated, end of life or missing
 * from the configuration, plus a {@code findRerankingProviders} test of the models an override can
 * name. They run in {@code FindAndRerankFakeRerankerIntegrationTest}.
 *
 * <p>For review: each configured model has its own fake endpoint and {@code max-batch-size} (3 for
 * {@code Model.SECOND}, 10 for the others), and the request carries the model name, so the tests
 * prove which reranker ran. Case JSON uses single quotes, which {@link #sendOverride} turns into
 * double quotes. The stored-model tests rewrite the table comment of {@code frr_comment} with CQL:
 * they depend on its internal format, briefly add a marker column, and restore the comment in a
 * {@code finally} block. On main a non-empty {@code authentication} or {@code parameters} is always
 * rejected before the reranker client, so only their errors and the null and {} forms are tested.
 */
public interface FindAndRerankOverrideCases extends FindAndRerankTestContext {

  /** Prefix of the {@code MethodSource} names of this interface. */
  String OVERRIDE_CASES =
      "io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankOverrideCases#";

  /** The query of {@link #sendOverride}. */
  String OVERRIDE_QUERY = "ChatGPT upgraded";

  /** The error for any non-empty {@code authentication} in an nvidia override. */
  String OVERRIDE_AUTH_ERROR =
      "Reranking provider 'nvidia' currently only supports 'NONE' or 'HEADER' authentication"
          + " types. No authentication parameters should be provided.";

  // ---- Successful requests ----

  // Overrides to the second model, then sends no override, then one naming the collection's own
  // model. Expects the second model, then the collection's model twice with identical requests. Per
  // the findAndRerank docs (Parameters, rerank) and issue #2459 an override is optional, needs only
  // provider and modelName, and applies to one request; the undocumented default model is pinned.
  @Test
  default void overrideIsOptionalAndAppliesToOneRequest() {
    assertOverrideReranked(MAIN, rerankOption(Model.SECOND), Model.SECOND, null);
    assertOverrideReranked(MAIN, null, Model.DEFAULT, null);
    assertOverrideReranked(MAIN, rerankOption(Model.DEFAULT), Model.DEFAULT, null);
  }

  // Sends a null rerank option, overrides with {} or null authentication and parameters, an
  // override with a field named "empty", and one with a reranking-api-key header. Expects success
  // on the override model (the collection's for null) with the usual headers; only that header
  // changes Authorization. The docs do not specify these cases; the test pins current behavior.
  @ParameterizedTest(name = "{0}")
  @MethodSource(OVERRIDE_CASES + "overrideAcceptedFormCases")
  default void overrideNullOrEmptyPartsAreAccepted(
      Fixture fixture, String rerank, Model model, String apiKey) {
    assertOverrideReranked(fixture, rerank, model, apiKey);
  }

  static Stream<Arguments> overrideAcceptedFormCases() {
    return Stream.of(
        Arguments.of(
            Named.of("rerank is null -> no override, collection model", MAIN),
            "null",
            Model.DEFAULT,
            null),
        overrideTo(
            "authentication and parameters are {} -> accepted",
            MAIN,
            Model.SECOND,
            "'authentication': {}, 'parameters': {}"),
        overrideTo(
            "authentication and parameters are null -> accepted",
            MAIN,
            Model.SECOND,
            "'authentication': null, 'parameters': null"),
        overrideTo("field named empty -> ignored", MAIN, Model.SECOND, "'empty': true"),
        Arguments.of(
            Named.of("reranking-api-key header -> Authorization uses that key", MAIN),
            rerankOption(Model.SECOND),
            Model.SECOND,
            "farr-override-key"));
  }

  // Sends overrides to the second and the default model, on collections with rerank on and off.
  // Expects only the override model's endpoint, name and batch size. Issue #2459 allows both; the
  // findAndRerank docs (Parameters, rerank) support only llama-3.2-nv-rerankqa-1b-v2, and the
  // hybrid search page starts from a collection with rerank on. The test pins current behavior;
  // whether it is a bug is still under discussion.
  @ParameterizedTest(name = "{0}")
  @MethodSource(OVERRIDE_CASES + "overrideSwitchCases")
  default void overrideSwitchesToAnotherConfiguredModel(
      Fixture fixture, String rerank, Model model, String apiKey) {
    assertOverrideReranked(fixture, rerank, model, apiKey);
  }

  static Stream<Arguments> overrideSwitchCases() {
    return Stream.of(
        overrideTo("rerank on, override to second model -> second model", MAIN, Model.SECOND),
        overrideTo("rerank off, override to default -> default model", NORERANK, Model.DEFAULT),
        overrideTo("rerank off, override to second model -> second model", NORERANK, Model.SECOND));
  }

  // Sends the request example of issue #2459 unchanged, plus a header that puts the read CQL in
  // status.trace. Expects the best ten of the twelve documents with a $vectorize text, all twelve
  // texts reranked (batches of 10 and 2), and LIMIT 50 on the only (vector) read. The docs
  // (Parameters) say hybridLimits defaults to limit and rerankOn to $lexical without $vector. The
  // test pins current behavior; whether it is a bug is still under discussion.
  @Test
  default void overrideIssue2459RequestExample() {
    ValidatableResponse response =
        postToFixture(
            MAIN,
            """
            {"findAndRerank": {
                "sort": {"$hybrid": {"$vectorize": "search text"}},
                "options": {"limit": 10, "rerank": {
                    "provider": "nvidia", "modelName": "nvidia/llama-3.2-nv-rerankqa-1b-v2"}}
            }}""",
            FindAndRerankRequests.headers(
                ApiFeature.REQUEST_TRACING_FULL.httpHeaderName(), "true"));
    assertIds(response, "m08", "m05", "m11", "m03", "m06", "m02", "m12", "m04", "m09", "m01");
    String[] twelve = "m01 m02 m03 m04 m05 m06 m07 m08 m09 m10 m11 m12".split(" ");
    assertRerankerCalls(defaultModelCalls("search text", passages(MAIN, "$vectorize", twelve)));
    Matcher limits = Pattern.compile("LIMIT (\\d+)").matcher(response.extract().asString());
    assertThat(limits.results().map(r -> r.group(1)).toList()).containsOnly("50");
  }

  // ---- Rejected overrides: each case expects one error, no data and no reranker call ----

  // Sends overrides that are incomplete or have the wrong JSON type; numbers and booleans are read
  // as text first. Per issue #2459 (Notes: the override must be complete) and the findAndRerank
  // docs (Parameters, rerank: an object with the strings provider and modelName) these overrides
  // are invalid; the error codes themselves are not documented.
  @ParameterizedTest(name = "{0}")
  @MethodSource(OVERRIDE_CASES + "overrideDocumentedRejections")
  default void overrideRejectedAsDocumented(
      Fixture fixture, String rerank, ErrorCode<?> code, String absent, String[] snippets) {
    assertOverrideRejected(fixture, rerank, code, absent, snippets);
  }

  static Stream<Arguments> overrideDocumentedRejections() {
    String noProvider = "Provider name is required for reranking service configuration";
    return Stream.of(
        // Incomplete overrides.
        invalid("rerank is {} -> provider required", "{}", noProvider),
        rejectedOn(
            "rerank is {} on a collection with rerank disabled -> provider required",
            NORERANK,
            "{}",
            INVALID_RERANK_OVERRIDE,
            noProvider),
        invalid(
            "all four fields are null -> provider required",
            "{'provider': null, 'modelName': null, 'authentication': null, 'parameters': null}",
            noProvider),
        invalid(
            "provider missing -> provider required",
            "{'modelName': 'nvidia/llama-3.2-nv-rerankqa-1b-v2'}",
            noProvider),
        invalid(
            "modelName missing -> model name required",
            "{'provider': 'nvidia'}",
            "Model name is required for reranking provider 'nvidia'"),
        unknownField("unknown field model -> COMMAND_FIELD_UNKNOWN", "model", "'x'"),
        // The rerank option is not an object.
        mismatch("rerank is 'nvidia' -> structure mismatch", "'nvidia'"),
        mismatch("rerank is '' -> structure mismatch", "''"),
        mismatch("rerank is 1 -> structure mismatch", "1"),
        mismatch("rerank is true -> structure mismatch", "true"),
        mismatch("rerank is [] -> structure mismatch", "[]"),
        // A number or boolean name is read as text; an object or array name is not.
        invalid(
            "provider is 123 -> read as text, not supported",
            "{'provider': 123, 'modelName': 'm'}",
            "Reranking provider '123' is not supported"),
        invalid(
            "provider is true -> read as text, not supported",
            "{'provider': true, 'modelName': 'm'}",
            "Reranking provider 'true' is not supported"),
        invalid(
            "modelName is 123 -> read as text, not supported",
            "{'provider': 'nvidia', 'modelName': 123}",
            "Model '123' is not supported by reranking provider"),
        invalid(
            "modelName is true -> read as text, not supported",
            "{'provider': 'nvidia', 'modelName': true}",
            "Model 'true' is not supported by reranking provider"),
        mismatch("provider is {} -> structure mismatch", "{'provider': {}, 'modelName': 'm'}"),
        mismatch(
            "provider is ['nvidia'] -> structure mismatch",
            "{'provider': ['nvidia'], 'modelName': 'm'}"),
        mismatch(
            "modelName is {} -> structure mismatch", "{'provider': 'nvidia', 'modelName': {}}"),
        mismatch(
            "modelName is ['nvidia'] -> structure mismatch",
            "{'provider': 'nvidia', 'modelName': ['nvidia']}"));
  }

  // Sends overrides the docs do not describe: wrong JSON types inside authentication or parameters,
  // near-miss and very long names, a disabled provider, models without a configured status message,
  // a SHARED_SECRET-only provider, and overrides that break two rules (the case name says which
  // error wins). The docs do not specify these cases; the test pins current behavior.
  @ParameterizedTest(name = "{0}")
  @MethodSource(OVERRIDE_CASES + "overrideUndocumentedRejections")
  default void overrideRejectedInUndocumentedCases(
      Fixture fixture, String rerank, ErrorCode<?> code, String absent, String[] snippets) {
    assertOverrideRejected(fixture, rerank, code, absent, snippets);
  }

  static Stream<Arguments> overrideUndocumentedRejections() {
    String withAuthentication = "'authentication': {'providerKey': 'my-test-key'}";
    return Stream.of(
        rejectedOn(
            "rerank is null on a collection with rerank disabled -> no override",
            NORERANK,
            "null",
            UNSUPPORTED_RERANKING_COMMAND,
            "a reranking service override was not provided with the command"),
        // authentication and parameters must be objects; authentication values must be scalars.
        mismatchWith("authentication is 'abc' -> structure mismatch", "'authentication': 'abc'"),
        mismatchWith("authentication is '' -> structure mismatch", "'authentication': ''"),
        mismatchWith("authentication is 1 -> structure mismatch", "'authentication': 1"),
        mismatchWith("authentication is [] -> structure mismatch", "'authentication': []"),
        mismatchWith(
            "authentication value is an object -> structure mismatch",
            "'authentication': {'providerKey': {'a': 1}}"),
        mismatchWith(
            "authentication value is an array -> structure mismatch",
            "'authentication': {'providerKey': ['a']}"),
        mismatchWith("parameters is 'abc' -> structure mismatch", "'parameters': 'abc'"),
        mismatchWith("parameters is '' -> structure mismatch", "'parameters': ''"),
        mismatchWith("parameters is 1 -> structure mismatch", "'parameters': 1"),
        mismatchWith("parameters is [] -> structure mismatch", "'parameters': []"),
        // Model names that differ from a configured one only in case or spaces, and long names.
        unsupportedModel(
            "modelName 'NVIDIA/LLAMA-3.2-NV-RERANKQA-1B-V2' -> not supported, names match exactly",
            "NVIDIA/LLAMA-3.2-NV-RERANKQA-1B-V2"),
        unsupportedModel(
            "modelName ' nvidia/llama-3.2-nv-rerankqa-1b-v2' -> not supported, names match exactly",
            " nvidia/llama-3.2-nv-rerankqa-1b-v2"),
        unsupportedModel("modelName '' -> not supported, names match exactly", ""),
        unsupportedProvider(
            "provider name of 3000 characters -> no length check, not supported", "x".repeat(3000)),
        unsupportedModel(
            "modelName of 3000 characters -> no length check, not supported", "x".repeat(3000)),
        // Disabled providers, models without a configured status message, no reranking client.
        invalid(
            "disabled provider with its model -> provider disabled",
            rerankOption(Model.JINA),
            "Reranking provider 'jinaAI' is disabled"),
        rejected(
            "DEPRECATED model without a configured message -> default text",
            rerankOption(Model.DEPRECATED_NO_MESSAGE),
            DEPRECATED_AI_MODEL,
            "It is at DEPRECATED status.",
            "The model is DEPRECATED."),
        rejected(
            "END_OF_LIFE model without a configured message -> default text",
            rerankOption(Model.EOL_NO_MESSAGE),
            END_OF_LIFE_AI_MODEL,
            "It is at END_OF_LIFE status.",
            "The model is END_OF_LIFE."),
        rejected(
            "provider with only SHARED_SECRET, with authentication -> no reranking client",
            rerankOption(Model.COHERE, withAuthentication),
            RERANKING_SERVICE_TYPE_UNAVAILABLE,
            "Reranking service type unavailable: unknown service provider 'cohere'."),
        rejectedOn(
            "provider without reranking client, no rerankOn or rerankQuery -> client error",
            BYO,
            rerankOption(Model.VOYAGE),
            RERANKING_SERVICE_TYPE_UNAVAILABLE,
            "unknown service provider 'voyageAI'"),
        // Overrides that break two rules; the description says which error wins.
        invalidWithout(
            "unknown provider without modelName -> provider error, not model error",
            "{'provider': 'unknown-provider'}",
            "Reranking provider 'unknown-provider' is not supported",
            "Model name is required"),
        invalidWithout(
            "disabled provider without modelName -> provider disabled, not model error",
            "{'provider': 'jinaAI'}",
            "Reranking provider 'jinaAI' is disabled",
            "Model name is required"),
        invalidWithout(
            "disabled provider with an unknown model -> provider disabled, not model error",
            "{'provider': 'jinaAI', 'modelName': 'jina/unknown'}",
            "Reranking provider 'jinaAI' is disabled",
            "is not supported"),
        invalidWithout(
            "unknown model with authentication -> model error first",
            "{'provider': 'nvidia', 'modelName': 'nvidia/unknown', " + withAuthentication + "}",
            "Model 'nvidia/unknown' is not supported by reranking provider 'nvidia'",
            "authentication types"),
        rejected(
            "DEPRECATED model with parameters -> model error first",
            rerankOption(Model.DEPRECATED, "'parameters': {'truncate': 'END'}"),
            DEPRECATED_AI_MODEL,
            "It is at DEPRECATED status."),
        rejected(
            "END_OF_LIFE model with authentication -> model error first",
            rerankOption(Model.EOL, withAuthentication),
            END_OF_LIFE_AI_MODEL,
            "It is at END_OF_LIFE status."));
  }

  // Sends a non-empty authentication or parameters, and the key "auth". Issue #2459 says "auth" and
  // parameters can be overridden; the findAndRerank docs (Parameters, rerank) list only provider
  // and modelName. main rejects the key "auth", any parameters, and any authentication when the
  // provider lists NONE or HEADER, enabled or not (nvidia lists NONE, disabled here; mistral lists
  // only HEADER). The test pins current behavior; whether it is a bug is still under discussion.
  @ParameterizedTest(name = "{0}")
  @MethodSource(OVERRIDE_CASES + "overrideAuthenticationRejections")
  default void overrideAuthenticationOrParametersRejected(
      Fixture fixture, String rerank, ErrorCode<?> code, String absent, String[] snippets) {
    assertOverrideRejected(fixture, rerank, code, absent, snippets);
  }

  static Stream<Arguments> overrideAuthenticationRejections() {
    return Stream.of(
        authRejected(
            "authentication is {'providerKey': 'my-test-key'} -> rejected",
            "{'providerKey': 'my-test-key'}"),
        authRejected("authentication is {'providerKey': 123} -> rejected", "{'providerKey': 123}"),
        authRejected(
            "authentication is {'providerKey': true} -> rejected", "{'providerKey': true}"),
        authRejected(
            "authentication is {'providerKey': null} -> rejected", "{'providerKey': null}"),
        authRejected(
            "authentication is {'SHARED_SECRET': 'x'} -> rejected", "{'SHARED_SECRET': 'x'}"),
        authRejected("authentication is {'foo': 'bar'} -> rejected", "{'foo': 'bar'}"),
        invalid(
            "provider with only HEADER authentication, with authentication -> rejected",
            rerankOption(Model.MISTRAL, "'authentication': {'providerKey': 'my-test-key'}"),
            "Reranking provider 'mistral' currently only supports 'NONE' or 'HEADER'"
                + " authentication types. No authentication parameters should be provided."),
        paramsRejected("parameters is {'truncate': 'END'} -> rejected", "{'truncate': 'END'}"),
        paramsRejected("parameters is {'truncate': null} -> rejected", "{'truncate': null}"),
        invalidWithout(
            "authentication and parameters both set -> authentication error only",
            rerankOption(
                Model.DEFAULT,
                "'authentication': {'providerKey': 'k'}, 'parameters': {'truncate': 'END'}"),
            OVERRIDE_AUTH_ERROR,
            "doesn't support any parameters"),
        unknownField("auth is null -> unknown field, the key is authentication", "auth", "null"),
        unknownField("auth is {} -> unknown field, the key is authentication", "auth", "{}"),
        unknownField(
            "auth is {'providerKey': 'my-test-key'} -> unknown field, the key is authentication",
            "auth",
            "{'providerKey': 'my-test-key'}"));
  }

  // Sends unknown or near-miss provider and model names (main matches names exactly), providers
  // without a reranking client, and the DEPRECATED and END_OF_LIFE models of the test YAML. Issue
  // #2459 says provider and model can be replaced; the findAndRerank docs (Parameters, rerank)
  // support only "Nvidia" (their example writes "nvidia") and llama-3.2-nv-rerankqa-1b-v2 (without
  // "nvidia/"). The test pins current behavior; whether it is a bug is still under discussion.
  @ParameterizedTest(name = "{0}")
  @MethodSource(OVERRIDE_CASES + "overrideProviderOrModelRejections")
  default void overrideProviderOrModelRejected(
      Fixture fixture, String rerank, ErrorCode<?> code, String absent, String[] snippets) {
    assertOverrideRejected(fixture, rerank, code, absent, snippets);
  }

  static Stream<Arguments> overrideProviderOrModelRejections() {
    String unknown = "{'provider': 'unknown-provider', 'modelName': 'some-model'}";
    String unknownText = "Reranking provider 'unknown-provider' is not supported";
    return Stream.of(
        invalid("unknown provider -> not supported", unknown, unknownText),
        rejectedOn(
            "unknown provider on a collection with rerank disabled -> not supported",
            NORERANK,
            unknown,
            INVALID_RERANK_OVERRIDE,
            unknownText),
        unsupportedProvider("provider 'Nvidia' -> not supported, names match exactly", "Nvidia"),
        unsupportedProvider("provider 'NVIDIA' -> not supported, names match exactly", "NVIDIA"),
        unsupportedProvider("provider ' nvidia' -> not supported, names match exactly", " nvidia"),
        unsupportedProvider("provider '' -> not supported, names match exactly", ""),
        rejected(
            "configured provider without a reranking client -> service type unavailable",
            rerankOption(Model.VOYAGE),
            RERANKING_SERVICE_TYPE_UNAVAILABLE,
            "Reranking service type unavailable: unknown service provider 'voyageAI'."),
        rejected(
            "configured provider the code does not know -> server error",
            rerankOption(Model.FAKE_PROVIDER),
            UNEXPECTED_SERVER_ERROR,
            "Error Class: IllegalArgumentException",
            "Error Message: Unknown reranking service provider 'fakeProvider'"),
        unsupportedModel("unknown nvidia model -> not supported", "nvidia/unknown"),
        unsupportedModel(
            "model name without nvidia/, as in the docs -> not supported",
            "llama-3.2-nv-rerankqa-1b-v2"),
        rejected(
            "DEPRECATED model -> DEPRECATED_AI_MODEL with the configured message",
            rerankOption(Model.DEPRECATED),
            DEPRECATED_AI_MODEL,
            "The command attempted to create or alter a collection or table",
            "The model is: nvidia/a-random-deprecated-model. It is at DEPRECATED status.",
            "This model has been deprecated, it will be removed in a future release."),
        rejected(
            "END_OF_LIFE model -> END_OF_LIFE_AI_MODEL with the configured message",
            rerankOption(Model.EOL),
            END_OF_LIFE_AI_MODEL,
            "The model is: nvidia/a-random-EOL-model. It is at END_OF_LIFE status.",
            "This model is at END_OF_LIFE status, it is not supported."));
  }

  // ---- Collections whose stored model changed after creation (rewritten with CQL) ----

  // Stored model DEPRECATED. Sends no override, an override naming that model, then one to the
  // second model. Expects the stored model without a warning, DEPRECATED_AI_MODEL, then the second
  // model. Undocumented state; issue #2459 lets an override replace the reranker. The test pins
  // current behavior; whether rejecting the model in use is a bug is still under discussion.
  @Test
  default void overrideOnStoredDeprecatedModel() {
    withStoredRerankService(
        service -> service.put("modelName", Model.DEPRECATED.modelName()),
        () -> {
          assertOverrideReranked(COMMENT, null, Model.DEPRECATED, null)
              .body("status.warnings", nullValue());
          assertApiError(
              sendOverride(COMMENT, rerankOption(Model.DEPRECATED), null),
              DEPRECATED_AI_MODEL,
              "It is at DEPRECATED status.");
          assertOverrideReranked(COMMENT, rerankOption(Model.SECOND), Model.SECOND, null);
        });
  }

  // Stored model END_OF_LIFE, with or without a configured message. Sends no override, an override
  // naming that model, then one to the second model. Expects END_OF_LIFE_AI_MODEL twice (default
  // texts differ without a message), then the second model. Undocumented state; issue #2459 lets an
  // override replace the reranker. The test pins current behavior; whether rejecting the same model
  // is a bug is still under discussion.
  @ParameterizedTest(name = "{0}")
  @MethodSource(OVERRIDE_CASES + "overrideStoredEndOfLifeCases")
  default void overrideOnStoredEndOfLifeModel(
      Model stored, String storedText, String overrideText) {
    withStoredRerankService(
        service -> service.put("modelName", stored.modelName()),
        () -> {
          assertApiError(
              sendOverride(COMMENT, null, null),
              END_OF_LIFE_AI_MODEL,
              "The model is: %s. It is at END_OF_LIFE status.".formatted(stored.modelName()),
              storedText);
          assertApiError(
              sendOverride(COMMENT, rerankOption(stored), null),
              END_OF_LIFE_AI_MODEL,
              overrideText);
          assertOverrideReranked(COMMENT, rerankOption(Model.SECOND), Model.SECOND, null);
        });
  }

  static Stream<Arguments> overrideStoredEndOfLifeCases() {
    String configured = "This model is at END_OF_LIFE status, it is not supported.";
    return Stream.of(
        Arguments.of(
            Named.of("END_OF_LIFE model with a configured message", Model.EOL),
            configured,
            configured),
        Arguments.of(
            Named.of("END_OF_LIFE model without a message -> two texts", Model.EOL_NO_MESSAGE),
            "The model is no longer supported (reached its end-of-life).",
            "The model is END_OF_LIFE."));
  }

  // Stored provider or model name missing from the reranking configuration. Sends no override, then
  // one to the second model. Expects UNEXPECTED_SERVER_ERROR (a NullPointerException before any
  // reranker client exists), then the second model. Undocumented state; issue #2459 lets an
  // override replace the reranker. The test pins current behavior; whether the server error is a
  // bug is still under discussion.
  @ParameterizedTest(name = "{0}")
  @MethodSource(OVERRIDE_CASES + "overrideStoredUnknownCases")
  default void overrideOnStoredModelMissingFromConfig(
      String field, String value, String errorMessage) {
    withStoredRerankService(
        service -> service.put(field, value),
        () -> {
          assertApiError(
              sendOverride(COMMENT, null, null),
              UNEXPECTED_SERVER_ERROR,
              "Error Class: NullPointerException",
              errorMessage);
          assertOverrideReranked(COMMENT, rerankOption(Model.SECOND), Model.SECOND, null);
        });
  }

  static Stream<Arguments> overrideStoredUnknownCases() {
    return Stream.of(
        Arguments.of(
            Named.of("stored model name is not configured", "modelName"),
            "nvidia/no-such-model",
            "modelConfig filtered from rerankServiceDef must not be null"),
        Arguments.of(
            Named.of("stored provider is not configured", "provider"),
            "noSuchProvider",
            "providerConfig filtered from rerankServiceDef must not be null"));
  }

  // ---- Models an override can name ----

  // Sends findRerankingProviders without options. Expects the enabled providers (not the disabled
  // jinaAI) and only the SUPPORTED nvidia models, sorted by name, the default model once; the test
  // configuration also has DEPRECATED and END_OF_LIFE nvidia models, one named like the default
  // model. The docs do not specify this; the test pins current behavior.
  @Test
  default void findRerankingProvidersWithoutOptionsListsOnlySupportedModels() {
    ValidatableResponse response =
        postToDatabase("{\"findRerankingProviders\": {}}", FindAndRerankRequests.headers());
    response.statusCode(200).body("$", responseIsStatusOnly());
    assertThat(response.extract().<Map<String, Object>>path("status.rerankingProviders"))
        .containsOnlyKeys("cohere", "fakeProvider", "mistral", "nvidia", "voyageAI");
    String nvidiaModels = "status.rerankingProviders.nvidia.models.";
    assertThat(response.extract().<List<String>>path(nvidiaModels + "name"))
        .containsExactly(
            "nvidia/farr-bad-url",
            "nvidia/farr-capped-back-off",
            "nvidia/farr-connection-refused",
            "nvidia/farr-fast-timeout",
            "nvidia/farr-growing-back-off",
            "nvidia/farr-negative-batch",
            "nvidia/farr-second-model",
            "nvidia/farr-unknown-host",
            "nvidia/farr-zero-retries",
            "nvidia/llama-3.2-nv-rerankqa-1b-v2");
    assertThat(response.extract().<List<String>>path(nvidiaModels + "apiModelSupport.status"))
        .containsOnly("SUPPORTED");
    assertRerankerNotCalled();
  }

  // ---- Helpers ----

  /**
   * Clears the fake reranker and sends a {@code $vectorize} sort (a {@code $vector} on {@code
   * frr_byo}), filter {@code grp: "plain"} on {@code frr_main}, the rerank option with its single
   * quotes turned into double quotes, and the {@code reranking-api-key} header unless null.
   */
  private ValidatableResponse sendOverride(Fixture fixture, String rerank, String apiKey) {
    FakeRerankerClient.reset();
    var request =
        fixture == MAIN ? findAndRerank().filter("{\"grp\": \"plain\"}") : findAndRerank();
    request.sort(
        fixture == BYO
            ? "{\"$hybrid\": {\"$vector\": " + EXAMPLE_VECTOR + "}}"
            : "{\"$hybrid\": {\"$vectorize\": \"" + OVERRIDE_QUERY + "\"}}");
    if (rerank != null) {
      request.options("{\"rerank\": " + rerank.replace('\'', '"') + "}");
    }
    return postToFixture(
        fixture,
        request.json(),
        FindAndRerankRequests.headers(
            HttpConstants.RERANKING_AUTHENTICATION_TOKEN_HEADER_NAME, apiKey));
  }

  /**
   * Sends the override; the response holds {@link #rerankedIds} in order, and the reranker got
   * their {@code $vectorize} texts on the endpoint of {@code model}, in batches of its size, with
   * {@code Bearer apiKey} or the token.
   */
  private ValidatableResponse assertOverrideReranked(
      Fixture fixture, String rerank, Model model, String apiKey) {
    ValidatableResponse response = sendOverride(fixture, rerank, apiKey);
    String[] ids = rerankedIds(fixture);
    assertIds(response, (Object[]) ids);
    int size = model == Model.SECOND ? 3 : 10;
    Integer[] batches = new Integer[(ids.length + size - 1) / size];
    for (int i = 0; i < batches.length; i++) {
      batches[i] = Math.min(size, ids.length - i * size);
    }
    var expected =
        calls(batches.length)
            .on(model)
            .query(OVERRIDE_QUERY)
            .passages(passages(fixture, "$vectorize", ids))
            .batchSizes(batches);
    assertRerankerCalls(apiKey == null ? expected : expected.rerankingApiKey(apiKey));
    return response;
  }

  /** The _ids {@link #sendOverride} reranks, best score first. */
  private static String[] rerankedIds(Fixture fixture) {
    if (fixture == MAIN) {
      return new String[] {"m08", "m11", "m06", "m12", "m09", "m07", "m10"};
    }
    return fixture == NORERANK ? new String[] {"r1", "r3", "r2"} : new String[] {"k2", "k1"};
  }

  /**
   * Rewrites the stored rerank service of {@code frr_comment}, waits until a request without an
   * override is no longer reranked by the default model (the change is in effect), runs {@code
   * checks}, then restores the comment and waits until the default model gets requests again.
   */
  private void withStoredRerankService(Consumer<ObjectNode> edit, Runnable checks) {
    TableCommentRewriter.withRewrittenComment(
        keyspace(),
        ensure(COMMENT),
        comment -> edit.accept((ObjectNode) comment.at("/collection/options/rerank/service")),
        () -> sendOverride(COMMENT, null, null),
        response -> FakeRerankerClient.requests(Model.DEFAULT).isEmpty(),
        response -> checks.run());
  }

  /** Sends the override and checks the error; the message must not contain {@code absent}. */
  private void assertOverrideRejected(
      Fixture fixture, String rerank, ErrorCode<?> code, String absent, String[] snippets) {
    ValidatableResponse response = sendOverride(fixture, rerank, null);
    assertApiError(response, code, snippets);
    if (absent != null) {
      response.body("errors[0].message", not(containsString(absent)));
    }
  }

  /** The rerank option that names this model, plus extra fields written with single quotes. */
  private static String rerankOption(Model model, String... extraFields) {
    String extra = extraFields.length == 0 ? "" : ", " + String.join(", ", extraFields);
    return "{'provider': '%s', 'modelName': '%s'%s}"
        .formatted(model.provider(), model.modelName(), extra);
  }

  private static Arguments overrideTo(
      String description, Fixture fixture, Model model, String... extraFields) {
    return Arguments.of(
        Named.of(description, fixture), rerankOption(model, extraFields), model, null);
  }

  // Rejected cases. Arguments: collection, rerank option, error code, a text the message must not
  // contain (or null), and the texts it must contain. Without a collection it is frr_main.

  private static Arguments invalid(String description, String rerank, String snippet) {
    return rejected(description, rerank, INVALID_RERANK_OVERRIDE, snippet);
  }

  private static Arguments unsupportedProvider(String description, String provider) {
    return invalid(
        description,
        "{'provider': '%s', 'modelName': '%s'}".formatted(provider, Model.DEFAULT.modelName()),
        "Reranking provider '%s' is not supported".formatted(provider));
  }

  private static Arguments unsupportedModel(String description, String modelName) {
    return invalid(
        description,
        "{'provider': 'nvidia', 'modelName': '%s'}".formatted(modelName),
        "Model '%s' is not supported by reranking provider 'nvidia'".formatted(modelName));
  }

  private static Arguments authRejected(String description, String authentication) {
    return invalid(
        description,
        rerankOption(Model.DEFAULT, "'authentication': " + authentication),
        OVERRIDE_AUTH_ERROR);
  }

  private static Arguments paramsRejected(String description, String parameters) {
    return invalid(
        description,
        rerankOption(Model.DEFAULT, "'parameters': " + parameters),
        "Reranking provider 'nvidia' currently doesn't support any parameters. No parameters"
            + " should be provided.");
  }

  private static Arguments unknownField(String description, String field, String value) {
    String known = "('authentication', 'modelName', 'parameters', 'provider')";
    return rejected(
        description,
        rerankOption(Model.DEFAULT, "'%s': %s".formatted(field, value)),
        COMMAND_FIELD_UNKNOWN,
        "Command field '%s' not recognized: not one of known fields %s".formatted(field, known));
  }

  private static Arguments invalidWithout(
      String description, String rerank, String snippet, String absent) {
    return rejection(description, MAIN, rerank, INVALID_RERANK_OVERRIDE, absent, snippet);
  }

  private static Arguments mismatch(String description, String rerank) {
    return rejected(
        description,
        rerank,
        REQUEST_STRUCTURE_MISMATCH,
        "Request is valid JSON but has a structural mismatch");
  }

  private static Arguments mismatchWith(String description, String extraField) {
    return mismatch(description, rerankOption(Model.DEFAULT, extraField));
  }

  private static Arguments rejected(
      String description, String rerank, ErrorCode<?> code, String... snippets) {
    return rejection(description, MAIN, rerank, code, null, snippets);
  }

  private static Arguments rejectedOn(
      String description, Fixture fixture, String rerank, ErrorCode<?> code, String snippet) {
    return rejection(description, fixture, rerank, code, null, snippet);
  }

  private static Arguments rejection(
      String description,
      Fixture fixture,
      String rerank,
      ErrorCode<?> code,
      String absent,
      String... snippets) {
    return Arguments.of(Named.of(description, fixture), rerank, code, absent, snippets);
  }
}
