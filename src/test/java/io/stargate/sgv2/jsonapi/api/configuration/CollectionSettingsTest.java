package io.stargate.sgv2.jsonapi.api.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSettings;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import java.util.Collections;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class CollectionSettingsTest {
  final ObjectMapper objectMapper = new ObjectMapper();

  @Test
  public void ensureSingleProjectorCreation() {
    CollectionSettings.IndexingConfig indexingConfig =
        new CollectionSettings.IndexingConfig(Collections.singleton("abc"), null);
    CollectionSettings settings =
        new CollectionSettings("collectionName", false, -1, null, null, null, indexingConfig);
    DocumentProjector indexingProj = settings.indexingProjector();
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
