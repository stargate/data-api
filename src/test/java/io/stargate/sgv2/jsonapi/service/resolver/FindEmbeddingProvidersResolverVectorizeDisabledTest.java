package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.util.asserts.DataAPIAsserts.assertThatSchemaException;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindEmbeddingProvidersCommand;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.operation.embeddings.FindEmbeddingProvidersOperation;
import io.stargate.sgv2.jsonapi.service.schema.DatabaseSchemaObject;
import io.stargate.sgv2.jsonapi.util.profiles.DisableVectorizeProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(DisableVectorizeProfile.class)
public class FindEmbeddingProvidersResolverVectorizeDisabledTest
    extends CommandResolverTestBase<
        DatabaseSchemaObject,
        FindEmbeddingProvidersCommand,
        FindEmbeddingProvidersCommandResolver,
        FindEmbeddingProvidersOperation> {

  @Inject FindEmbeddingProvidersCommandResolver RESOLVER;
  private final CommandContext<DatabaseSchemaObject> COMMAND_CONTEXT =
      TEST_CONSTANTS.databaseContext();

  @Override
  protected FindEmbeddingProvidersCommandResolver resolver() {
    return RESOLVER;
  }

  @Override
  protected CommandContext<DatabaseSchemaObject> commandContext() {
    return COMMAND_CONTEXT;
  }

  @Override
  protected Class<FindEmbeddingProvidersCommand> commandClass() {
    return FindEmbeddingProvidersCommand.class;
  }

  @Test
  public void failVectorizeSearchDisabled() {

    var throwable =
        assertResolverThrows(
            "failVectorizeSearchDisabled()",
            """
                            {
                              "findEmbeddingProviders": {}
                            }
                            """);

    assertThatSchemaException(throwable)
        .as("failVectorizeSearchDisabled()")
        .hasCode(SchemaException.Code.VECTORIZE_FEATURE_NOT_AVAILABLE);
  }
}
