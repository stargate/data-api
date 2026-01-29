package io.stargate.sgv2.jsonapi.service.processor;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CountDocumentsCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.api.request.tenant.TenantFactory;
import io.stargate.sgv2.jsonapi.exception.ServerException;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class MeteredCommandProcessorTest {
  @Inject MeteredCommandProcessor meteredCommandProcessor;
  @InjectMock protected CommandProcessor commandProcessor;
  @InjectMock protected RequestContext requestContext;
  @Inject ObjectMapper objectMapper;

  private final TestConstants testConstants = new TestConstants();

  CommandContext<CollectionSchemaObject> commandContext;
  TenantFactory tenantFactory;

  @BeforeEach
  public void beforeEach() {
    commandContext = testConstants.collectionContext();
    tenantFactory = TenantFactory.instance();
  }

  @Nested
  class CustomMetrics {
    @Test
    public void metrics() throws Exception {
      String json =
          """
          {
            "countDocuments": {

            }
          }
          """;

      CountDocumentsCommand countCommand =
          objectMapper.readValue(json, CountDocumentsCommand.class);

      CommandResult commandResult = CommandResult.statusOnlyBuilder(RequestTracing.NO_OP).build();

      Mockito.when(commandProcessor.processCommand(commandContext, countCommand))
          .thenReturn(Uni.createFrom().item(commandResult));

      meteredCommandProcessor
          .processCommand(commandContext, countCommand)
          .await()
          .atMost(Duration.ofMinutes(1));
      String metrics = given().when().get("/metrics").then().statusCode(200).extract().asString();

      List<String> httpMetrics =
          metrics
              .lines()
              .filter(
                  line ->
                      line.startsWith("command_processor_process")
                          && !line.startsWith("command_processor_process_seconds_bucket")
                          && !line.contains("quantile")
                          && line.contains("error=\"false\""))
              .toList();

      assertThat(httpMetrics)
          .satisfies(
              lines -> {
                assertThat(lines.size()).isEqualTo(3);
                lines.forEach(
                    line -> {
                      assertThat(line).contains("command=\"CountDocumentsCommand\"");
                      assertThat(line)
                          .contains("tenant=\"%s".formatted(testConstants.TENANT.toString()));
                      assertThat(line).contains("error=\"false\"");
                      assertThat(line).contains("error_code=\"NA\"");
                      assertThat(line).contains("module=\"sgv2-jsonapi\"");
                    });
              });
    }

    @Test
    public void errorMetricsWithNoErrorCode() throws Exception {
      String json =
          """
        {
          "find": {

          }
        }
        """;

      FindCommand countCommand = objectMapper.readValue(json, FindCommand.class);

      // easier to create an Exception and build the error from it, it will include tags etc
      var exception =
          ServerException.Code.INTERNAL_SERVER_ERROR.get("errorMessage", "test error details");
      var commandResult =
          CommandResult.statusOnlyBuilder(RequestTracing.NO_OP).addThrowable(exception).build();

      Mockito.when(commandProcessor.processCommand(commandContext, countCommand))
          .thenReturn(Uni.createFrom().item(commandResult));
      meteredCommandProcessor
          .processCommand(commandContext, countCommand)
          .await()
          .atMost(Duration.ofMinutes(1));

      String metrics = given().when().get("/metrics").then().statusCode(200).extract().asString();
      List<String> httpMetrics =
          metrics
              .lines()
              .filter(
                  line ->
                      line.startsWith("command_processor_process_seconds_")
                          && !line.contains("seconds_bucket")
                          && line.contains("error=\"true\"")
                          && !line.startsWith("command_processor_process_seconds_bucket")
                          && !line.contains("quantile")
                          && line.contains("command=\"FindCommand\""))
              .toList();

      assertThat(httpMetrics)
          .satisfies(
              lines -> {
                assertThat(lines.size()).isEqualTo(3);
                lines.forEach(
                    line -> {
                      assertThat(line).contains("command=\"FindCommand\"");
                      assertThat(line)
                          .contains("tenant=\"%s".formatted(testConstants.TENANT.toString()));

                      assertThat(line).contains("error=\"true\"");
                      assertThat(line)
                          .contains("error_code=\"" + exception.fullyQualifiedCode() + "\"");
                      assertThat(line).contains("module=\"sgv2-jsonapi\"");
                    });
              });
    }

    @Test
    public void errorMetrics() throws Exception {
      String json =
          """
          {
            "countDocuments": {

            }
          }
          """;

      CountDocumentsCommand countCommand =
          objectMapper.readValue(json, CountDocumentsCommand.class);

      // easier to create an Exception and build the error from it, it will include tags etc
      var exception =
          ServerException.Code.INTERNAL_SERVER_ERROR.get("errorMessage", "test error details");
      var commandResult =
          CommandResult.statusOnlyBuilder(RequestTracing.NO_OP).addThrowable(exception).build();
      ;

      Mockito.when(commandProcessor.processCommand(commandContext, countCommand))
          .thenReturn(Uni.createFrom().item(commandResult));
      meteredCommandProcessor
          .processCommand(commandContext, countCommand)
          .await()
          .atMost(Duration.ofMinutes(1));

      // amorton - 14 jan 2026 - there is a timing problem, if we get metrics too quickly they are
      // not ready
      Thread.sleep(1000);

      String metrics = given().when().get("/metrics").then().statusCode(200).extract().asString();
      List<String> httpMetrics =
          metrics
              .lines()
              .filter(
                  line ->
                      line.startsWith("command_processor_process_")
                          && line.contains("error=\"true\"")
                          && !line.startsWith("command_processor_process_seconds_bucket")
                          && !line.contains("quantile")
                          && line.contains("command=\"CountDocumentsCommand\""))
              .toList();

      assertThat(httpMetrics)
          .satisfies(
              lines -> {
                assertThat(lines.size()).isEqualTo(3);
                lines.forEach(
                    line -> {
                      assertThat(line).contains("command=\"CountDocumentsCommand\"");
                      assertThat(line)
                          .contains("tenant=\"%s".formatted(testConstants.TENANT.toString()));
                      assertThat(line).contains("error=\"true\"");
                      assertThat(line)
                          .contains("error_code=\"" + exception.fullyQualifiedCode() + "\"");
                      assertThat(line).contains("module=\"sgv2-jsonapi\"");
                    });
              });
    }
  }
}
