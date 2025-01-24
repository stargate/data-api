package io.stargate.sgv2.jsonapi.service.resolver;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertOneCommand;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.InsertCollectionOperation;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentShredder;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class InsertOneCommandResolverTest {

  @Inject ObjectMapper objectMapper;
  @Inject InsertOneCommandResolver resolver;
  @Inject DocumentShredder documentShredder;
  @InjectMock protected DataApiRequestInfo dataApiRequestInfo;

  @Nested
  class ResolveCommand {

    CommandContext<CollectionSchemaObject> commandContext = TestConstants.collectionContext();

    @Test
    public void happyPath() throws Exception {
      String json =
          """
          {
            "insertOne": {
              "document" : {
                "_id": "1"
              }
            }
          }
          """;

      InsertOneCommand command = objectMapper.readValue(json, InsertOneCommand.class);
      Operation result = resolver.resolveCommand(commandContext, command);

      assertThat(result)
          .isInstanceOfSatisfying(
              InsertCollectionOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.ordered()).isFalse();
                assertThat(op.insertions()).hasSize(1);
              });
    }

    @Test
    public void happyPathWithVector() throws Exception {
      String json =
          """
        {
          "insertOne": {
            "document" : {
              "_id": "1",
              "$vector" : [0.11,0.22,0.33,0.44]
            }
          }
        }
        """;

      InsertOneCommand command = objectMapper.readValue(json, InsertOneCommand.class);
      Operation result = resolver.resolveCommand(commandContext, command);

      assertThat(result)
          .isInstanceOfSatisfying(
              InsertCollectionOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.ordered()).isFalse();
                assertThat(op.insertions()).hasSize(1);
              });
    }
  }
}
