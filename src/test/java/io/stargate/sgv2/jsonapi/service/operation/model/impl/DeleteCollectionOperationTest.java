package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.sgv2.common.bridge.AbstractValidatingStargateBridgeTest;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.service.bridge.executor.QueryExecutor;
import java.util.function.Supplier;
import javax.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class DeleteCollectionOperationTest extends AbstractValidatingStargateBridgeTest {
  @Inject QueryExecutor queryExecutor;

  @Nested
  class Execute {

    @Test
    public void happyPath() throws Exception {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      CommandContext commandContext = new CommandContext(namespace, null);

      String cql = "DROP TABLE IF EXISTS \"%s\".\"%s\";".formatted(namespace, collection);
      withQuery(cql).returningNothing();

      DeleteCollectionOperation operation =
          new DeleteCollectionOperation(commandContext, collection);

      Supplier<CommandResult> result =
          operation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .assertCompleted()
              .getItem();

      assertThat(result.get())
          .satisfies(
              commandResult -> {
                assertThat(commandResult.status().get(CommandStatus.OK)).isEqualTo(1);
              });
    }

    @Test
    public void happyPathCaseSensitive() throws Exception {
      String namespace = RandomStringUtils.randomAlphanumeric(16).toUpperCase();
      String collection = RandomStringUtils.randomAlphanumeric(16).toUpperCase();
      CommandContext commandContext = new CommandContext(namespace, null);

      String cql = "DROP TABLE IF EXISTS \"%s\".\"%s\";".formatted(namespace, collection);
      withQuery(cql).returningNothing();

      DeleteCollectionOperation operation =
          new DeleteCollectionOperation(commandContext, collection);

      Supplier<CommandResult> result =
          operation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .assertCompleted()
              .getItem();

      assertThat(result.get())
          .satisfies(
              commandResult -> {
                assertThat(commandResult.status().get(CommandStatus.OK)).isEqualTo(1);
              });
    }
  }
}
