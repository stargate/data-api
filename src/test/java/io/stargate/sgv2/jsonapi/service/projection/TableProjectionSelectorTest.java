package io.stargate.sgv2.jsonapi.service.projection;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.fixtures.testdata.TableMetadataTestData;
import io.stargate.sgv2.jsonapi.fixtures.testdata.TestData;
import io.stargate.sgv2.jsonapi.fixtures.testdata.TestDataNames;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class TableProjectionSelectorTest {

  private static final TestData TEST_DATA = new TestData();
  private static final TestDataNames NAMES = TEST_DATA.names;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static TableSchemaObject realTableAllTypes() {
    var tm = new TableMetadataTestData(TEST_DATA).tableAllDatatypesNotIndexed();
    return TableSchemaObject.from(tm, new ObjectMapper());
  }

  private static TableProjectionDefinition include(String json) {
    try {
      return TableProjectionDefinition.createFromDefinition(OBJECT_MAPPER.readTree(json));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Nested
  class NonUdtSelectors {
    @Test
    public void inclusion_keeps_only_listed_columns() {
      // Tests that inclusion projection selects only specified columns
      var table = realTableAllTypes();
      var def =
          include(
              "{"
                  + "\""
                  + NAMES.CQL_INT_COLUMN.asInternal()
                  + "\":1,"
                  + "\""
                  + NAMES.CQL_TEXT_COLUMN.asInternal()
                  + "\":1"
                  + "}");

      var selectors = TableProjectionSelectors.from(def, table);
      var selected = selectors.toCqlColumns();
      assertThat(selected)
          .extracting(c -> c.getName())
          .contains(NAMES.CQL_INT_COLUMN, NAMES.CQL_TEXT_COLUMN);
      assertThat(selected).extracting(c -> c.getName()).doesNotContain(NAMES.CQL_BOOLEAN_COLUMN);
    }

    @Test
    public void exclusion_removes_listed_columns() {
      // Tests that exclusion projection removes specified columns
      var table = realTableAllTypes();
      var def = include("{" + "\"" + NAMES.CQL_TEXT_COLUMN.asInternal() + "\":0" + "}");

      var selectors = TableProjectionSelectors.from(def, table);
      var selected = selectors.toCqlColumns();
      assertThat(selected).extracting(c -> c.getName()).doesNotContain(NAMES.CQL_TEXT_COLUMN);
    }
  }

  @Nested
  class UdtSelectors {
    private String udtCol() {
      return NAMES.CQL_NON_FROZEN_UDT_COLUMN_ADDRESS.asInternal();
    }

    private String cityField() {
      return NAMES.CQL_ADDRESS_CITY_FIELD.asInternal();
    }

    private String countryField() {
      return NAMES.CQL_ADDRESS_COUNTRY_FIELD.asInternal();
    }

    @Test
    public void inclusion_subfield_creates_udt_selector_with_field() {
      // Tests that UDT subfield inclusion creates selector with only that field
      var table = realTableAllTypes();
      var def = include("{\"" + udtCol() + "." + cityField() + "\":1}");

      var selectors = TableProjectionSelectors.from(def, table);
      var selector =
          (TableUDTProjectionSelector)
              selectors.getSelectorForColumn(CqlIdentifier.fromInternal(udtCol()));
      assertThat(selector).isNotNull();
      assertThat(selector.getSubFields()).contains(cityField());
      assertThat(selectors.toCqlColumns())
          .extracting(c -> c.getName().asInternal())
          .contains(udtCol());
    }

    @Test
    public void inclusion_multiple_subfields_creates_udt_selector_with_both() {
      // Tests that multiple UDT subfield inclusions create selector with all fields
      var table = realTableAllTypes();
      var def =
          include(
              "{\""
                  + udtCol()
                  + "."
                  + cityField()
                  + "\":1, \""
                  + udtCol()
                  + "."
                  + countryField()
                  + "\":1}");

      var selectors = TableProjectionSelectors.from(def, table);
      var selector =
          (TableUDTProjectionSelector)
              selectors.getSelectorForColumn(CqlIdentifier.fromInternal(udtCol()));
      assertThat(selector).isNotNull();
      assertThat(selector.getSubFields()).contains(cityField(), countryField());
    }

    @Test
    public void inclusion_whole_udt_overrides_subfields() {
      // Tests that whole UDT inclusion overrides subfield specifications
      var table = realTableAllTypes();
      var def = include("{\"" + udtCol() + "\":1, \"" + udtCol() + "." + cityField() + "\":1}");

      var selectors = TableProjectionSelectors.from(def, table);
      var selector = selectors.getSelectorForColumn(CqlIdentifier.fromInternal(udtCol()));
      assertThat(selector).isInstanceOf(TableUDTProjectionSelector.class);
      var full =
          new ObjectMapper()
              .createObjectNode()
              .put(cityField(), "New York")
              .put(countryField(), "USA");
      var projected = selector.projectToJsonNode(full);
      assertThat(projected.get(cityField()).asText()).isEqualTo("New York");
      assertThat(projected.get(countryField()).asText()).isEqualTo("USA");
    }

    @Test
    public void exclusion_subfield_removes_field_and_keeps_others() {
      // Tests that UDT subfield exclusion removes only that field, keeps others
      var table = realTableAllTypes();
      var def = include("{\"" + udtCol() + "." + cityField() + "\":0}");

      var selectors = TableProjectionSelectors.from(def, table);
      var selector =
          (TableUDTProjectionSelector)
              selectors.getSelectorForColumn(CqlIdentifier.fromInternal(udtCol()));
      assertThat(selector).isNotNull();
      assertThat(selector.getSubFields()).doesNotContain(cityField());
      assertThat(selector.getSubFields()).contains(countryField());
    }

    @Test
    public void exclusion_all_subfields_removes_udt_selector() {
      // Tests that excluding all UDT subfields removes the UDT selector entirely
      var table = realTableAllTypes();
      var def =
          include(
              "{\""
                  + udtCol()
                  + "."
                  + cityField()
                  + "\":0, \""
                  + udtCol()
                  + "."
                  + countryField()
                  + "\":0}");

      var selectors = TableProjectionSelectors.from(def, table);
      assertThat(selectors.getSelectorForColumn(CqlIdentifier.fromInternal(udtCol()))).isNull();
    }

    @Test
    public void exclusion_whole_udt_removes_udt_selector() {
      // Tests that whole UDT exclusion removes the UDT selector entirely
      var table = realTableAllTypes();
      var def = include("{\"" + udtCol() + "\":0}");

      var selectors = TableProjectionSelectors.from(def, table);
      assertThat(selectors.getSelectorForColumn(CqlIdentifier.fromInternal(udtCol()))).isNull();
    }

    @Test
    public void exclusion_projection_removes_specified_fields() {
      // Tests that exclusion projection removes all specified UDT subfields
      var table = realTableAllTypes();
      var def =
          include(
              "{\""
                  + udtCol()
                  + "."
                  + cityField()
                  + "\":0, \""
                  + udtCol()
                  + "."
                  + countryField()
                  + "\":0}");

      var selectors = TableProjectionSelectors.from(def, table);
      var selector =
          (TableUDTProjectionSelector)
              selectors.getSelectorForColumn(CqlIdentifier.fromInternal(udtCol()));
      // since all fields are excluded, the udt selector should be null
      assertThat(selector).isNull();
    }

    @Test
    public void udt_projection_with_null_values_excludes_nulls() {
      // Tests that null UDT field values are excluded from projection results
      var table = realTableAllTypes();
      var def =
          include(
              "{\""
                  + udtCol()
                  + "."
                  + cityField()
                  + "\":1, \""
                  + udtCol()
                  + "."
                  + countryField()
                  + "\":1}");

      var selectors = TableProjectionSelectors.from(def, table);
      var selector =
          (TableUDTProjectionSelector)
              selectors.getSelectorForColumn(CqlIdentifier.fromInternal(udtCol()));
      assertThat(selector).isNotNull();

      // Test with null city field
      var partial =
          new ObjectMapper().createObjectNode().putNull(cityField()).put(countryField(), "USA");
      var projected = selector.projectToJsonNode(partial);
      assertThat(projected.has(cityField())).isFalse();
      assertThat(projected.get(countryField()).asText()).isEqualTo("USA");
    }
  }
}
