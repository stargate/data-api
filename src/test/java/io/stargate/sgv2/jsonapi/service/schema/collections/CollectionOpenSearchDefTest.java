package io.stargate.sgv2.jsonapi.service.schema.collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class CollectionOpenSearchDefTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Nested
  class ApiFeatureFlagTest {

    @Test
    void apiFeatureProperties() {
      assertThat(ApiFeature.OPEN_SEARCH.featureName()).isEqualTo("open-search");
      assertThat(ApiFeature.OPEN_SEARCH.httpHeaderName()).isEqualTo("Feature-Flag-open-search");
      assertThat(ApiFeature.OPEN_SEARCH.enabledByDefault()).isFalse();
    }
  }

  @Nested
  class WhenOpenSearchFeatureDisabled {

    private final CollectionOpenSearchDefSchemaFactory factory =
        CollectionOpenSearchDefSchemaFactory.FOR_TESTING_DISABLED;

    @Test
    void shouldReturnDisabledWhenNoDescProvided() {
      var holder = CollectionOpenSearchDef.fromApiDesc(null, "my_ks", "my_coll", factory);

      assertThat(holder).isNotNull();
      assertThat(holder.runningValue().enabled()).isFalse();
    }

    @Test
    void shouldReturnDisabledWhenExplicitlyDisabled() {
      var desc = new CreateCollectionCommand.Options.OpenSearchDesc(false, null, null, null, null);
      var holder = CollectionOpenSearchDef.fromApiDesc(desc, "my_ks", "my_coll", factory);

      assertThat(holder).isNotNull();
      assertThat(holder.runningValue().enabled()).isFalse();
    }

    @Test
    void shouldFailWhenTryingToEnableOpenSearch() {
      ObjectNode mappings = OBJECT_MAPPER.createObjectNode();
      mappings.putObject("title").put("type", "text");

      var desc =
          new CreateCollectionCommand.Options.OpenSearchDesc(true, null, null, null, mappings);

      assertThatThrownBy(
              () -> CollectionOpenSearchDef.fromApiDesc(desc, "my_ks", "my_coll", factory))
          .isInstanceOf(SchemaException.class)
          .hasFieldOrPropertyWithValue(
              "code", SchemaException.Code.OPEN_SEARCH_FEATURE_NOT_ENABLED.name());
    }
  }

  @Nested
  class WhenOpenSearchFeatureEnabled {

    private final CollectionOpenSearchDefSchemaFactory factory =
        CollectionOpenSearchDefSchemaFactory.FOR_TESTING_ENABLED;

    @Test
    void shouldReturnDisabledWhenNoDescProvided() {
      var holder = CollectionOpenSearchDef.fromApiDesc(null, "my_ks", "my_coll", factory);

      assertThat(holder).isNotNull();
      assertThat(holder.runningValue().enabled()).isFalse();
    }

    @Test
    void shouldReturnDisabledWhenExplicitlyDisabled() {
      var desc = new CreateCollectionCommand.Options.OpenSearchDesc(false, null, null, null, null);
      var holder = CollectionOpenSearchDef.fromApiDesc(desc, "my_ks", "my_coll", factory);

      assertThat(holder).isNotNull();
      assertThat(holder.runningValue().enabled()).isFalse();
    }

    @Test
    void shouldSucceedWhenOpenSearchEnabledWithValidMappings() {
      ObjectNode mappings = OBJECT_MAPPER.createObjectNode();
      mappings.putObject("title").put("type", "text");

      var desc =
          new CreateCollectionCommand.Options.OpenSearchDesc(true, null, null, null, mappings);

      var holder = CollectionOpenSearchDef.fromApiDesc(desc, "my_ks", "my_coll", factory);
      assertThat(holder).isNotNull();
      assertThat(holder.runningValue().enabled()).isTrue();
      assertThat(holder.runningValue().saiIndexName()).isEqualTo("hcd_my_ks_my_coll");
    }
  }
}
