package io.stargate.sgv2.jsonapi.service.schema.collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.IndexKind;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import java.util.LinkedHashMap;
import java.util.Map;
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
      assertThat(holder.runningValue().hasCustomMappings()).isTrue();
    }

    @Test
    void shouldSucceedWhenOpenSearchEnabledWithoutMappings() {
      var desc = new CreateCollectionCommand.Options.OpenSearchDesc(true, null, null, null, null);

      var def =
          CollectionOpenSearchDef.fromApiDesc(desc, "my_ks", "my_coll", factory).runningValue();
      assertThat(def.enabled()).isTrue();
      assertThat(def.indexName()).isEqualTo("hcd_my_ks_my_coll");
      assertThat(def.saiIndexName()).isEqualTo("hcd_my_ks_my_coll");
      assertThat(def.mappings()).isNull();
      assertThat(def.hasCustomMappings()).isFalse();
    }

    @Test
    void shouldTreatEmptyMappingsObjectAsNoCustomMappings() {
      var desc =
          new CreateCollectionCommand.Options.OpenSearchDesc(
              true, null, null, null, OBJECT_MAPPER.createObjectNode());

      var def =
          CollectionOpenSearchDef.fromApiDesc(desc, "my_ks", "my_coll", factory).runningValue();
      assertThat(def.enabled()).isTrue();
      assertThat(def.hasCustomMappings()).isFalse();
    }

    @Test
    void shouldStillRejectIdInMappings() {
      ObjectNode mappings = OBJECT_MAPPER.createObjectNode();
      mappings.putObject("_id").put("type", "keyword");
      var desc =
          new CreateCollectionCommand.Options.OpenSearchDesc(true, null, null, null, mappings);

      assertThatThrownBy(
              () -> CollectionOpenSearchDef.fromApiDesc(desc, "my_ks", "my_coll", factory))
          .isInstanceOf(SchemaException.class)
          .hasFieldOrPropertyWithValue(
              "code", SchemaException.Code.OPEN_SEARCH_MISSING_FIELD_MAPPINGS.name());
    }

    @Test
    void shouldStillRejectDollarPrefixedFieldInMappings() {
      ObjectNode mappings = OBJECT_MAPPER.createObjectNode();
      mappings.putObject("$vector").put("type", "knn_vector");
      var desc =
          new CreateCollectionCommand.Options.OpenSearchDesc(true, null, null, null, mappings);

      assertThatThrownBy(
              () -> CollectionOpenSearchDef.fromApiDesc(desc, "my_ks", "my_coll", factory))
          .isInstanceOf(SchemaException.class)
          .hasFieldOrPropertyWithValue(
              "code", SchemaException.Code.OPEN_SEARCH_MISSING_FIELD_MAPPINGS.name());
    }

    @Test
    void shouldStillRejectNonObjectMappings() {
      var desc =
          new CreateCollectionCommand.Options.OpenSearchDesc(
              true, null, null, null, OBJECT_MAPPER.createArrayNode().add("x"));

      assertThatThrownBy(
              () -> CollectionOpenSearchDef.fromApiDesc(desc, "my_ks", "my_coll", factory))
          .isInstanceOf(SchemaException.class)
          .hasFieldOrPropertyWithValue(
              "code", SchemaException.Code.OPEN_SEARCH_MISSING_FIELD_MAPPINGS.name());
    }
  }

  @Nested
  class ReadFromTableMetadata {

    private TableMetadata tableWithIndex(Map<String, String> options) {
      var index = mock(IndexMetadata.class);
      var allOptions = new LinkedHashMap<String, String>();
      allOptions.put("class_name", "OpenSearchIndex");
      allOptions.putAll(options);
      when(index.getKind()).thenReturn(IndexKind.CUSTOM);
      when(index.getOptions()).thenReturn(allOptions);
      when(index.getName()).thenReturn(CqlIdentifier.fromInternal("my_os_idx"));

      var table = mock(TableMetadata.class);
      when(table.getIndexes()).thenReturn(Map.of(CqlIdentifier.fromInternal("my_os_idx"), index));
      return table;
    }

    @Test
    void indexWithoutCustomMappingsIsAccepted() {
      var table =
          tableWithIndex(
              Map.of(
                  "indexName", "my_os",
                  "applyDefaultSchema", "false",
                  "applyCustomSchema", "false",
                  "unpackJsonFields", "doc_json"));

      var def = CollectionOpenSearchDef.fromTableMetadata(table, OBJECT_MAPPER);

      assertThat(def).isNotNull();
      assertThat(def.enabled()).isTrue();
      assertThat(def.indexName()).isEqualTo("my_os");
      assertThat(def.saiIndexName()).isEqualTo("my_os_idx");
      assertThat(def.mappings()).isNull();
      assertThat(def.hasCustomMappings()).isFalse();
    }

    @Test
    void indexWithEmptyPropertiesIsAcceptedAsNoCustomMappings() {
      var table =
          tableWithIndex(Map.of("indexName", "my_os", "customMappingsJson", "{\"properties\":{}}"));

      var def = CollectionOpenSearchDef.fromTableMetadata(table, OBJECT_MAPPER);

      assertThat(def).isNotNull();
      assertThat(def.hasCustomMappings()).isFalse();
    }

    @Test
    void indexWithCustomMappingsKeepsThemWithoutId() {
      var table =
          tableWithIndex(
              Map.of(
                  "indexName",
                  "my_os",
                  "customMappingsJson",
                  "{\"properties\":{\"title\":{\"type\":\"text\"},\"_id\":{\"type\":\"keyword\"}}}"));

      var def = CollectionOpenSearchDef.fromTableMetadata(table, OBJECT_MAPPER);

      assertThat(def).isNotNull();
      assertThat(def.hasCustomMappings()).isTrue();
      assertThat(def.mappings().has("title")).isTrue();
      assertThat(def.mappings().has("_id")).isFalse();
    }

    @Test
    void indexWithoutIndexNameIsCorrupt() {
      var table = tableWithIndex(Map.of("customMappingsJson", "{\"properties\":{}}"));

      assertThatThrownBy(() -> CollectionOpenSearchDef.fromTableMetadata(table, OBJECT_MAPPER))
          .isInstanceOf(SchemaException.class)
          .hasFieldOrPropertyWithValue(
              "code", SchemaException.Code.OPEN_SEARCH_CORRUPT_SCHEMA.name());
    }

    @Test
    void indexWithUnparseableCustomMappingsIsCorrupt() {
      var table = tableWithIndex(Map.of("indexName", "my_os", "customMappingsJson", "{not-json"));

      assertThatThrownBy(() -> CollectionOpenSearchDef.fromTableMetadata(table, OBJECT_MAPPER))
          .isInstanceOf(SchemaException.class)
          .hasFieldOrPropertyWithValue(
              "code", SchemaException.Code.OPEN_SEARCH_CORRUPT_SCHEMA.name());
    }
  }

  @Nested
  class ReadFromCommentJson {

    @Test
    void commentWithoutMappingsIsAccepted() throws Exception {
      var node = OBJECT_MAPPER.readTree("{\"enabled\":true,\"indexName\":\"my_os\"}");

      var def = CollectionOpenSearchDef.fromCommentJson(node, "ks", "coll", OBJECT_MAPPER);

      assertThat(def.enabled()).isTrue();
      assertThat(def.indexName()).isEqualTo("my_os");
      assertThat(def.saiIndexName()).isEqualTo("hcd_ks_coll");
      assertThat(def.hasCustomMappings()).isFalse();
    }

    @Test
    void commentWithoutIndexNameIsCorrupt() throws Exception {
      var node = OBJECT_MAPPER.readTree("{\"enabled\":true}");

      assertThatThrownBy(
              () -> CollectionOpenSearchDef.fromCommentJson(node, "ks", "coll", OBJECT_MAPPER))
          .isInstanceOf(SchemaException.class)
          .hasFieldOrPropertyWithValue(
              "code", SchemaException.Code.OPEN_SEARCH_CORRUPT_SCHEMA.name());
    }

    @Test
    void commentWithNonObjectMappingsIsCorrupt() throws Exception {
      var node =
          OBJECT_MAPPER.readTree("{\"enabled\":true,\"indexName\":\"my_os\",\"mappings\":[1]}");

      assertThatThrownBy(
              () -> CollectionOpenSearchDef.fromCommentJson(node, "ks", "coll", OBJECT_MAPPER))
          .isInstanceOf(SchemaException.class)
          .hasFieldOrPropertyWithValue(
              "code", SchemaException.Code.OPEN_SEARCH_CORRUPT_SCHEMA.name());
    }
  }
}
