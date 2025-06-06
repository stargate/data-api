package io.stargate.sgv2.jsonapi.service.resolver;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertManyCommand;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.InsertCollectionOperation;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentShredder;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class InsertManyCommandResolverTest {

  @Inject ObjectMapper objectMapper;
  @Inject DocumentShredder documentShredder;
  @Inject InsertManyCommandResolver resolver;
  @InjectMock protected RequestContext dataApiRequestInfo;

  private TestConstants testConstants = new TestConstants();

  CommandContext<CollectionSchemaObject> commandContext;

  @BeforeEach
  public void beforeEach() {
    commandContext = testConstants.collectionContext();
  }

  @Nested
  class ResolveCommand {

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
              InsertCollectionOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.ordered()).isFalse();
                assertThat(op.insertions()).hasSize(2);
              });
    }

    @Test
    public void happyPathVectorSearch() throws Exception {
      String json =
          """
          {
            "insertMany": {
              "documents": [
                {
                  "_id": "1",
                  "location": "London",
                  "$vector" : [0.11,0.22]
                },
                {
                  "_id": "2",
                  "location": "New York",
                  "$vector" : [0.33,0.44]
                }
              ]
            }
          }
          """;

      InsertManyCommand command = objectMapper.readValue(json, InsertManyCommand.class);
      Operation result = resolver.resolveCommand(commandContext, command);

      assertThat(result)
          .isInstanceOfSatisfying(
              InsertCollectionOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.ordered()).isFalse();
                assertThat(op.insertions()).hasSize(2);
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
              InsertCollectionOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.ordered()).isFalse();
                assertThat(op.insertions()).hasSize(2);
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
                "ordered": true
              }
            }
          }
          """;

      InsertManyCommand command = objectMapper.readValue(json, InsertManyCommand.class);
      Operation result = resolver.resolveCommand(commandContext, command);

      assertThat(result)
          .isInstanceOfSatisfying(
              InsertCollectionOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.ordered()).isTrue();
                assertThat(op.insertions()).hasSize(2);
              });
    }
  }
}
