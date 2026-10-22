package io.stargate.sgv2.jsonapi.api.v1.response;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.quarkiverse.mcp.server.MetaKey;
import io.quarkiverse.mcp.server.TextContent;
import io.quarkiverse.mcp.server.ToolResponse;
import io.stargate.sgv2.jsonapi.api.model.command.CommandErrorFactory;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link CommandResult#toToolResponse(ObjectMapper)} verifying readable content and
 * the existing structured response mapping.
 */
public class CommandResultToToolResponseTest {

  private final ObjectMapper objectMapper = new ObjectMapper();

  @Test
  void successWithStatusOnly() throws Exception {
    var commandResult =
        CommandResult.statusOnlyBuilder(RequestTracing.NO_OP)
            .addStatus(CommandStatus.OK, 1)
            .build();

    ToolResponse response = commandResult.toToolResponse(objectMapper);

    assertThat(response.isError()).isFalse();
    assertReadableResult(response, commandResult);
    assertThat(response.structuredContent()).isNull();
    assertThat(response._meta())
        .containsExactlyEntriesOf(Map.of(MetaKey.of("status"), commandResult.status()));
  }

  @Test
  void successWithStatusAndData() throws Exception {
    var commandResult =
        CommandResult.singleDocumentBuilder(RequestTracing.NO_OP)
            .addStatus(CommandStatus.DELETED_COUNT, 0)
            .addDocument(null)
            .build();

    ToolResponse response = commandResult.toToolResponse(objectMapper);

    assertThat(response.isError()).isFalse();
    assertReadableResult(response, commandResult);
    assertThat(response.structuredContent()).isEqualTo(commandResult.data());
    assertThat(response._meta())
        .containsExactlyEntriesOf(Map.of(MetaKey.of("status"), commandResult.status()));
  }

  @Test
  void failWithErrorOnly() throws Exception {
    var commandResult =
        CommandResult.statusOnlyBuilder(RequestTracing.NO_OP)
            .addThrowable(new RuntimeException("test error"))
            .build();

    ToolResponse response = commandResult.toToolResponse(objectMapper);

    assertThat(response.isError()).isTrue();
    assertReadableResult(response, commandResult);
    assertThat(response.structuredContent()).isEqualTo(Map.of("errors", commandResult.errors()));
    assertThat(response._meta()).isEmpty();
  }

  @Test
  void failWithErrorAndStatus() throws Exception {
    var commandResult =
        CommandResult.statusOnlyBuilder(RequestTracing.NO_OP)
            .addStatus(CommandStatus.INSERTED_IDS, 1)
            .addThrowable(new RuntimeException("test error"))
            .build();

    ToolResponse response = commandResult.toToolResponse(objectMapper);

    assertThat(response.isError()).isTrue();
    assertReadableResult(response, commandResult);
    assertThat(response.structuredContent()).isEqualTo(Map.of("errors", commandResult.errors()));
    assertThat(response._meta())
        .containsExactlyEntriesOf(Map.of(MetaKey.of("status"), commandResult.status()));
  }

  @Test
  void successWithDocumentData() throws Exception {
    var document =
        objectMapper.createObjectNode().put("message", "Quotes: \"hello\"\nUnicode: café 🌟");
    document.putArray("values").add(0).add(true).addNull();
    var commandResult =
        CommandResult.singleDocumentBuilder(RequestTracing.NO_OP).addDocument(document).build();

    ToolResponse response = commandResult.toToolResponse(objectMapper);

    assertThat(response.isError()).isFalse();
    assertReadableResult(response, commandResult);
    assertThat(response.structuredContent()).isEqualTo(commandResult.data());
    assertThat(response._meta()).isEmpty();
  }

  @Test
  void successWithMultipleDocumentsAndPagination() throws Exception {
    var commandResult =
        CommandResult.multiDocumentBuilder(RequestTracing.NO_OP)
            .addDocument(objectMapper.createObjectNode().put("_id", "first"))
            .addDocument(objectMapper.createObjectNode().put("_id", "second"))
            .nextPageState("next-page")
            .build();

    ToolResponse response = commandResult.toToolResponse(objectMapper);

    assertThat(response.isError()).isFalse();
    assertReadableResult(response, commandResult);
    assertThat(response.structuredContent()).isEqualTo(commandResult.data());
    assertThat(response._meta()).isEmpty();
  }

  @Test
  void successWithEmptyResult() throws Exception {
    var commandResult = CommandResult.statusOnlyBuilder(RequestTracing.NO_OP).build();

    ToolResponse response = commandResult.toToolResponse(objectMapper);

    assertThat(response.isError()).isFalse();
    assertReadableResult(response, commandResult);
    assertThat(response.structuredContent()).isNull();
    assertThat(response._meta()).isEmpty();
  }

  @Test
  void errorsDoNotHidePartialDataFromReadableContent() throws Exception {
    var success =
        CommandResult.multiDocumentBuilder(RequestTracing.NO_OP)
            .addDocument(objectMapper.createObjectNode().put("_id", "successful-document"))
            .addStatus(CommandStatus.INSERTED_IDS, List.of("successful-document"))
            .build();
    var failure =
        CommandResult.statusOnlyBuilder(RequestTracing.NO_OP)
            .addThrowable(new RuntimeException("another document failed"))
            .build();
    // The builder suppresses data on errors. The response mapper must also handle partial results.
    var commandResult = new CommandResult(success.data(), success.status(), failure.errors());

    ToolResponse response = commandResult.toToolResponse(objectMapper);

    assertThat(response.isError()).isTrue();
    assertReadableResult(response, commandResult);
    assertThat(response.structuredContent()).isEqualTo(Map.of("errors", commandResult.errors()));
    assertThat(response._meta())
        .containsExactlyEntriesOf(Map.of(MetaKey.of("status"), commandResult.status()));
  }

  @Test
  void warningsRemainReadableWithoutMarkingResultAsError() throws Exception {
    var commandResult = CommandResult.statusOnlyBuilder(RequestTracing.NO_OP).build();
    commandResult.addWarning(CommandErrorFactory.create(new RuntimeException("test warning")));

    ToolResponse response = commandResult.toToolResponse(objectMapper);

    assertThat(response.isError()).isFalse();
    assertReadableResult(response, commandResult);
    assertThat(response.structuredContent()).isNull();
    assertThat(response._meta())
        .containsExactlyEntriesOf(Map.of(MetaKey.of("status"), commandResult.status()));
  }

  @Test
  void serializationFailureReturnsSafeReadableError() {
    String secret = "sensitive-payload-and-exception-details";
    var commandResult =
        CommandResult.singleDocumentBuilder(RequestTracing.NO_OP)
            .addDocument(objectMapper.createObjectNode().put("secret", secret))
            .addStatus(CommandStatus.INSERTED_IDS, List.of(secret))
            .build();
    var failingMapper = objectMapper.copy();
    failingMapper.registerModule(
        new SimpleModule()
            .addSerializer(
                CommandResult.class,
                new JsonSerializer<CommandResult>() {
                  @Override
                  public void serialize(
                      CommandResult value, JsonGenerator generator, SerializerProvider provider)
                      throws IOException {
                    generator.writeStartObject();
                    generator.writeStringField("partial", secret);
                    throw new IOException(secret);
                  }
                }));

    ToolResponse response = commandResult.toToolResponse(failingMapper);

    assertThat(response.isError()).isTrue();
    assertThat(readableText(response)).isNotBlank().hasSizeLessThan(200).doesNotContain(secret);
    assertThat(response.structuredContent()).isNull();
    assertThat(response._meta()).isNullOrEmpty();
  }

  private void assertReadableResult(ToolResponse response, CommandResult commandResult)
      throws IOException {
    assertThat(objectMapper.readTree(readableText(response)))
        .isEqualTo(objectMapper.valueToTree(commandResult));
  }

  private String readableText(ToolResponse response) {
    assertThat(response.content()).hasSize(1);
    return assertInstanceOf(TextContent.class, response.content().getFirst()).text();
  }
}
