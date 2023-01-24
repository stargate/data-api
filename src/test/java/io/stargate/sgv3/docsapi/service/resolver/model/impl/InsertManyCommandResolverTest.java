package io.stargate.sgv3.docsapi.service.resolver.model.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv3.docsapi.api.model.command.CommandContext;
import io.stargate.sgv3.docsapi.api.model.command.impl.InsertManyCommand;
import io.stargate.sgv3.docsapi.service.operation.model.Operation;
import io.stargate.sgv3.docsapi.service.operation.model.impl.InsertOperation;
import io.stargate.sgv3.docsapi.service.shredding.Shredder;
import io.stargate.sgv3.docsapi.service.shredding.model.WritableShreddedDocument;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class InsertManyCommandResolverTest {
  @Inject ObjectMapper objectMapper;
  @Inject Shredder shredder;
  @Inject InsertManyCommandResolver insertManyCommandResolver;

  @Nested
  class InsertManyResolveCommand {

    @Test
    public void idFilterCondition() throws Exception {
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

      InsertManyCommand insertManyCommand = objectMapper.readValue(json, InsertManyCommand.class);
      final CommandContext commandContext = new CommandContext("database", "collection");
      final Operation operation =
          insertManyCommandResolver.resolveCommand(commandContext, insertManyCommand);
      List<WritableShreddedDocument> shreddedDocuments =
          insertManyCommand.documents().stream()
              .map(doc -> shredder.shred(doc))
              .collect(Collectors.toList());
      InsertOperation expected = new InsertOperation(commandContext, shreddedDocuments);
      assertThat(operation)
          .isInstanceOf(InsertOperation.class)
          .satisfies(
              op -> {
                assertThat(op).isEqualTo(expected);
              });
    }
  }
}
