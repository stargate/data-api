package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.Mock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertManyCommand;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.InsertOperation;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import io.stargate.sgv2.jsonapi.service.shredding.model.WritableShreddedDocument;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class InsertManyCommandResolverTest {

  @Inject ObjectMapper objectMapper;
  @Inject Shredder shredder;
  @Inject InsertManyCommandResolver resolver;

  @Nested
  class ResolveCommand {

    @Mock CommandContext commandContext;

    @Test
    public void happyPath() throws Exception {
      String json =
          """
          {
            "insertMany": {
              "documents": [
                {
                  "_id": "1",
                  "location": "London"
                },
                {
                  "_id": "2",
                  "location": "New York"
                }
              ]
            }
          }
          """;

      InsertManyCommand command = objectMapper.readValue(json, InsertManyCommand.class);
      Operation result = resolver.resolveCommand(commandContext, command);

      assertThat(result)
          .isInstanceOfSatisfying(
              InsertOperation.class,
              op -> {
                WritableShreddedDocument first = shredder.shred(command.documents().get(0));
                WritableShreddedDocument second = shredder.shred(command.documents().get(1));

                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.ordered()).isTrue();
                assertThat(op.documents()).containsExactly(first, second);
              });
    }

    @Test
    public void optionsEmpty() throws Exception {
      String json =
          """
          {
            "insertMany": {
              "documents": [
                {
                  "_id": "1",
                  "location": "London"
                },
                {
                  "_id": "2",
                  "location": "New York"
                }
              ],
              "options": {
              }
            }
          }
          """;

      InsertManyCommand command = objectMapper.readValue(json, InsertManyCommand.class);
      Operation result = resolver.resolveCommand(commandContext, command);

      assertThat(result)
          .isInstanceOfSatisfying(
              InsertOperation.class,
              op -> {
                WritableShreddedDocument first = shredder.shred(command.documents().get(0));
                WritableShreddedDocument second = shredder.shred(command.documents().get(1));

                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.ordered()).isTrue();
                assertThat(op.documents()).containsExactly(first, second);
              });
    }

    @Test
    public void optionsNotOrdered() throws Exception {
      String json =
          """
          {
            "insertMany": {
              "documents": [
                {
                  "_id": "1",
                  "location": "London"
                },
                {
                  "_id": "2",
                  "location": "New York"
                }
              ],
              "options": {
                "ordered": false
              }
            }
          }
          """;

      InsertManyCommand command = objectMapper.readValue(json, InsertManyCommand.class);
      Operation result = resolver.resolveCommand(commandContext, command);

      assertThat(result)
          .isInstanceOfSatisfying(
              InsertOperation.class,
              op -> {
                WritableShreddedDocument first = shredder.shred(command.documents().get(0));
                WritableShreddedDocument second = shredder.shred(command.documents().get(1));

                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.ordered()).isFalse();
                assertThat(op.documents()).containsExactly(first, second);
              });
    }

    @Test
    public void shredderFailure() throws Exception {
      String json =
          """
          {
            "insertMany": {
              "documents": [
                {
                  "_id": "1",
                  "location": "London"
                },
                "primitive"
              ]
            }
          }
          """;

      InsertManyCommand command = objectMapper.readValue(json, InsertManyCommand.class);
      Throwable failure = catchThrowable(() -> resolver.resolveCommand(commandContext, command));

      assertThat(failure)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.SHRED_BAD_DOCUMENT_TYPE);
    }
  }
}
