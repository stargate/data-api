package io.stargate.sgv2.jsonapi.api.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorConfig;
import io.stargate.sgv2.jsonapi.service.projection.IndexingProjector;
import io.stargate.sgv2.jsonapi.service.schema.collections.*;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import java.util.Arrays;
import java.util.HashSet;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class CollectionSchemaObjectTest {
  @Test
  public void ensureSingleProjectorCreation() {
    CollectionIndexingConfig indexingConfig =
        new CollectionIndexingConfig(new HashSet<>(Arrays.asList("abc")), null);
    CollectionSchemaObject settings =
        new CollectionSchemaObject(
            "namespace",
            "collectionName",
            null,
            IdConfig.defaultIdConfig(),
            VectorConfig.NOT_ENABLED_CONFIG,
            indexingConfig,
            CollectionLexicalConfig.configForDisabled(),
            CollectionRerankingConfig.configForLegacyCollections());
    IndexingProjector indexingProj = settings.indexingProjector();
    assertThat(indexingProj)
        .isNotNull()
        // Should get the same instance second time due to memoization
        .isSameAs(settings.indexingProjector())
        // and third :)
        .isSameAs(settings.indexingProjector());

    // And should also work as expected: "abc" and below included; the rest, not
    assertThat(indexingProj.isPathIncluded("abc.x")).isTrue();
    assertThat(indexingProj.isPathIncluded("_id")).isFalse();
  }
}
