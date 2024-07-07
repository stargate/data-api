package io.stargate.sgv2.jsonapi.api.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.projection.IndexingProjector;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import java.util.Arrays;
import java.util.HashSet;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class CollectionSchemaObjectTest {
  final ObjectMapper objectMapper = new ObjectMapper();

  @Test
  public void ensureSingleProjectorCreation() {
    CollectionSchemaObject.IndexingConfig indexingConfig =
        new CollectionSchemaObject.IndexingConfig(new HashSet<String>(Arrays.asList("abc")), null);
    CollectionSchemaObject settings =
        new CollectionSchemaObject(
            "namespace",
            "collectionName",
            CollectionSchemaObject.IdConfig.defaultIdConfig(),
            CollectionSchemaObject.VectorConfig.notEnabledVectorConfig(),
            indexingConfig);
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
