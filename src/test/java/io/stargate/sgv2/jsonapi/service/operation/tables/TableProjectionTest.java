package io.stargate.sgv2.jsonapi.service.operation.tables;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import io.stargate.sgv2.jsonapi.fixtures.testdata.TableProjectionTestData;
import io.stargate.sgv2.jsonapi.fixtures.testdata.TestData;
import io.stargate.sgv2.jsonapi.fixtures.testdata.TestDataNames;
import java.math.BigDecimal;
import java.util.HashMap;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class TableProjectionTest {

  private static final TestData TEST_DATA = new TestData();
  private static final TestDataNames NAMES = TEST_DATA.names;

  private static TableProjectionTestData.Fixture tableWithAllTypes(String message) {
    return TEST_DATA.tableProjection().tableWithAllDataTypes(message);
  }

  private static TableProjectionTestData.Fixture tableWithUdt(String message) {
    return TEST_DATA.tableProjection().tableWithAllDataTypesPlusUdt(message);
  }

  private static UdtValue createUdtValueWithNullCity(String country) {
    // Get the UDT type from the table metadata
    var tableMetadata = TEST_DATA.tableMetadata().tableAllDatatypesNotIndexed();
    var udtColumn = tableMetadata.getColumn(NAMES.CQL_NON_FROZEN_UDT_COLUMN_ADDRESS).orElseThrow();
    var udtType = (UserDefinedType) udtColumn.getType();

    // Create a real UdtValue using the UDT type
    UdtValue udtValue = udtType.newValue();
    udtValue = udtValue.setToNull(NAMES.CQL_ADDRESS_CITY_FIELD);
    udtValue = udtValue.set(NAMES.CQL_ADDRESS_COUNTRY_FIELD, country, String.class);
    return udtValue;
  }

  @Nested
  class HighLevelProjection {

    @Test
    public void scalar_columns_inclusion_projects_all_types() {
      // Tests that inclusion projection works with various scalar data types
      var json =
          "{"
              + "\""
              + NAMES.CQL_TEXT_COLUMN.asInternal()
              + "\":1,"
              + "\""
              + NAMES.CQL_INT_COLUMN.asInternal()
              + "\":1,"
              + "\""
              + NAMES.CQL_BOOLEAN_COLUMN.asInternal()
              + "\":1,"
              + "\""
              + NAMES.CQL_DOUBLE_COLUMN.asInternal()
              + "\":1,"
              + "\""
              + NAMES.CQL_UUID_COLUMN.asInternal()
              + "\":1"
              + "}";

      var fixture =
          tableWithAllTypes("scalar columns inclusion projects all types")
              .withProjectionJson(json)
              .applySelect()
              .assertSelectContains(
                  NAMES.CQL_TEXT_COLUMN.asInternal(),
                  NAMES.CQL_INT_COLUMN.asInternal(),
                  NAMES.CQL_BOOLEAN_COLUMN.asInternal(),
                  NAMES.CQL_DOUBLE_COLUMN.asInternal(),
                  NAMES.CQL_UUID_COLUMN.asInternal());

      var out = fixture.projectRow();

      fixture
          .assertJsonHasString(out, NAMES.CQL_TEXT_COLUMN, "default_text")
          .assertJsonHasInt(out, NAMES.CQL_INT_COLUMN, 42)
          .assertJsonHasBoolean(out, NAMES.CQL_BOOLEAN_COLUMN, true)
          .assertJsonHasDouble(out, NAMES.CQL_DOUBLE_COLUMN, 3.14);
    }

    @Test
    public void scalar_columns_exclusion_removes_specified_types() {
      // Tests that exclusion projection removes specified scalar columns
      var json =
          "{"
              + "\""
              + NAMES.CQL_TEXT_COLUMN.asInternal()
              + "\":0,"
              + "\""
              + NAMES.CQL_BOOLEAN_COLUMN.asInternal()
              + "\":0"
              + "}";

      tableWithAllTypes("scalar columns exclusion removes specified types")
          .withProjectionJson(json)
          .applySelect()
          .assertSelectNotContains(
              NAMES.CQL_TEXT_COLUMN.asInternal(), NAMES.CQL_BOOLEAN_COLUMN.asInternal())
          .assertSelectContains(
              NAMES.CQL_INT_COLUMN.asInternal(), NAMES.CQL_DOUBLE_COLUMN.asInternal());
    }

    @Test
    public void null_scalar_values_are_not_returned() {
      // Tests that null scalar values are omitted from projection results
      var json =
          "{"
              + "\""
              + NAMES.CQL_TEXT_COLUMN.asInternal()
              + "\":1,"
              + "\""
              + NAMES.CQL_INT_COLUMN.asInternal()
              + "\":1,"
              + "\""
              + NAMES.CQL_BOOLEAN_COLUMN.asInternal()
              + "\":1"
              + "}";

      var fixture =
          tableWithAllTypes("null scalar values are not returned")
              .withProjectionJson(json)
              .applySelect();

      // Override specific values to null to test null handling
      var overrides = new HashMap<String, Object>();
      overrides.put(NAMES.CQL_TEXT_COLUMN.asInternal(), null);
      overrides.put(NAMES.CQL_BOOLEAN_COLUMN.asInternal(), null);
      var out = fixture.projectRowWithOverrides(overrides);

      fixture
          .assertJsonNodeSize(
              out, 1) // Only int column should be present (text and boolean are null)
          .assertJsonHasInt(out, NAMES.CQL_INT_COLUMN, 42)
          .assertJsonMissing(out, NAMES.CQL_TEXT_COLUMN, NAMES.CQL_BOOLEAN_COLUMN);
    }

    @Test
    public void numeric_types_projection_works_correctly() {
      // Tests that various numeric types project correctly
      var json =
          "{"
              + "\""
              + NAMES.CQL_INT_COLUMN.asInternal()
              + "\":1,"
              + "\""
              + NAMES.CQL_BIGINT_COLUMN.asInternal()
              + "\":1,"
              + "\""
              + NAMES.CQL_DOUBLE_COLUMN.asInternal()
              + "\":1,"
              + "\""
              + NAMES.CQL_FLOAT_COLUMN.asInternal()
              + "\":1,"
              + "\""
              + NAMES.CQL_DECIMAL_COLUMN.asInternal()
              + "\":1"
              + "}";

      var fixture =
          tableWithAllTypes("numeric types projection works correctly")
              .withProjectionJson(json)
              .applySelect()
              .assertSelectContains(
                  NAMES.CQL_INT_COLUMN.asInternal(),
                  NAMES.CQL_BIGINT_COLUMN.asInternal(),
                  NAMES.CQL_DOUBLE_COLUMN.asInternal(),
                  NAMES.CQL_FLOAT_COLUMN.asInternal(),
                  NAMES.CQL_DECIMAL_COLUMN.asInternal());

      var out = fixture.projectRow();

      fixture
          .assertJsonNodeSize(out, 5) // Should have exactly 5 numeric fields
          .assertJsonHasInt(out, NAMES.CQL_INT_COLUMN, 42)
          .assertJsonHasLong(out, NAMES.CQL_BIGINT_COLUMN, 123456789L)
          .assertJsonHasDouble(out, NAMES.CQL_DOUBLE_COLUMN, 3.14)
          .assertJsonHasDouble(out, NAMES.CQL_FLOAT_COLUMN, 2.5)
          .assertJsonHasBigDecimal(out, NAMES.CQL_DECIMAL_COLUMN, BigDecimal.valueOf(123.45));
    }

    @Test
    public void date_time_types_projection_works_correctly() {
      // Tests that date/time types project correctly
      var json =
          "{"
              + "\""
              + NAMES.CQL_DATE_COLUMN.asInternal()
              + "\":1,"
              + "\""
              + NAMES.CQL_TIMESTAMP_COLUMN.asInternal()
              + "\":1,"
              + "\""
              + NAMES.CQL_TIME_COLUMN.asInternal()
              + "\":1"
              + "}";

      var fixture =
          tableWithAllTypes("date/time types projection works correctly")
              .withProjectionJson(json)
              .applySelect()
              .assertSelectContains(
                  NAMES.CQL_DATE_COLUMN.asInternal(),
                  NAMES.CQL_TIMESTAMP_COLUMN.asInternal(),
                  NAMES.CQL_TIME_COLUMN.asInternal());

      var out = fixture.projectRow();

      // Verify that date/time fields are present and check size
      fixture
          .assertJsonNodeSize(out, 3) // Should have exactly 3 fields
          .assertJsonHasObject(out, NAMES.CQL_DATE_COLUMN)
          .assertJsonHasObject(out, NAMES.CQL_TIMESTAMP_COLUMN)
          .assertJsonHasObject(out, NAMES.CQL_TIME_COLUMN);
    }

    @Test
    public void mixed_inclusion_exclusion_throws_error() {
      // Tests that mixing inclusion and exclusion throws validation error
      var json =
          "{"
              + "\""
              + NAMES.CQL_INT_COLUMN.asInternal()
              + "\":1,"
              + "\""
              + NAMES.CQL_TEXT_COLUMN.asInternal()
              + "\":0"
              + "}";

      assertThat(
              assertThrows(
                  RuntimeException.class,
                  () ->
                      tableWithAllTypes("mixed inclusion/exclusion should throw error")
                          .withProjectionJson(json)
                          .applySelect()))
          .hasMessageContaining("cannot exclude");
    }

    @Test
    public void inclusion_selects_only_listed_columns_and_projects_non_nulls() {
      // Tests that inclusion projection selects only specified columns and excludes null values
      var json =
          "{"
              + "\""
              + NAMES.CQL_TEXT_COLUMN.asInternal()
              + "\":1,"
              + "\""
              + NAMES.CQL_INT_COLUMN.asInternal()
              + "\":1"
              + "}";

      var fixture =
          tableWithAllTypes("inclusion projects only selected non-null fields")
              .withProjectionJson(json)
              .applySelect()
              .assertSelectContains(
                  NAMES.CQL_TEXT_COLUMN.asInternal(), NAMES.CQL_INT_COLUMN.asInternal())
              .assertSelectNotContains(NAMES.CQL_BOOLEAN_COLUMN.asInternal());

      // Override text column to null to test null exclusion
      var overrides = new HashMap<String, Object>();
      overrides.put(NAMES.CQL_TEXT_COLUMN.asInternal(), null);
      var out = fixture.projectRowWithOverrides(overrides);
      fixture
          .assertJsonHasInt(out, NAMES.CQL_INT_COLUMN, 42)
          .assertJsonMissing(out, NAMES.CQL_TEXT_COLUMN);
    }

    @Test
    public void exclusion_prunes_listed_columns_from_selection() {
      // Tests that exclusion projection removes specified columns from CQL SELECT
      var json = "{" + "\"" + NAMES.CQL_TEXT_COLUMN.asInternal() + "\":0" + "}";

      tableWithAllTypes("exclusion removes selected columns from SELECT")
          .withProjectionJson(json)
          .applySelect()
          .assertSelectNotContains(NAMES.CQL_TEXT_COLUMN.asInternal())
          .assertSelectContains(NAMES.CQL_INT_COLUMN.asInternal());
    }

    @Test
    public void null_and_empty_collections_are_not_returned() {
      // Tests that null and empty collection values are omitted from projection results
      var json =
          "{"
              + "\""
              + NAMES.CQL_INT_COLUMN.asInternal()
              + "\":1,"
              + "\""
              + NAMES.CQL_SET_COLUMN.asInternal()
              + "\":1,"
              + "\""
              + NAMES.CQL_MAP_COLUMN.asInternal()
              + "\":1,"
              + "\""
              + NAMES.CQL_LIST_COLUMN.asInternal()
              + "\":1"
              + "}";

      var fixture =
          tableWithAllTypes("null and empty collection values omitted")
              .withProjectionJson(json)
              .applySelect();

      // Override collection values to null to test null handling
      var overrides = new HashMap<String, Object>();
      overrides.put(NAMES.CQL_SET_COLUMN.asInternal(), null);
      overrides.put(NAMES.CQL_MAP_COLUMN.asInternal(), null);
      overrides.put(NAMES.CQL_LIST_COLUMN.asInternal(), null);
      var out = fixture.projectRowWithOverrides(overrides);

      fixture
          .assertJsonHasInt(out, NAMES.CQL_INT_COLUMN, 42)
          .assertJsonMissing(
              out, NAMES.CQL_SET_COLUMN, NAMES.CQL_MAP_COLUMN, NAMES.CQL_LIST_COLUMN);
    }
  }

  @Nested
  class UdtProjection {

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
    public void inclusion_udt_top_level_includes_all_fields() {
      // Tests that UDT top-level inclusion includes all UDT fields
      var json = "{\"" + udtCol() + "\":1}";

      var fixture =
          tableWithUdt("inclusion UDT top level includes all fields")
              .withProjectionJson(json)
              .applySelect()
              .assertSelectContains(udtCol());

      var out = fixture.projectRow();

      fixture.assertJsonHasObject(out, NAMES.CQL_NON_FROZEN_UDT_COLUMN_ADDRESS);
      var projectedUdt = out.get(NAMES.CQL_NON_FROZEN_UDT_COLUMN_ADDRESS.asInternal());
      assertThat(projectedUdt.get(cityField()).asText()).isEqualTo("default_" + cityField());
      assertThat(projectedUdt.get(countryField()).asText()).isEqualTo("default_" + countryField());
    }

    @Test
    public void inclusion_udt_sub_level_includes_only_specified_fields() {
      // Tests that UDT subfield inclusion includes only specified fields
      var json = "{\"" + udtCol() + "." + cityField() + "\":1}";

      var fixture =
          tableWithUdt("inclusion UDT sub level includes only specified fields")
              .withProjectionJson(json)
              .applySelect()
              .assertSelectContains(udtCol());

      var out = fixture.projectRow();
      fixture.assertJsonHasObject(out, NAMES.CQL_NON_FROZEN_UDT_COLUMN_ADDRESS);
      var projectedUdt = out.get(NAMES.CQL_NON_FROZEN_UDT_COLUMN_ADDRESS.asInternal());
      assertThat(projectedUdt.get(cityField()).asText()).isEqualTo("default_" + cityField());
      assertThat(projectedUdt.has(countryField())).isFalse();
    }

    @Test
    public void inclusion_udt_top_level_overrides_sub_level() {
      // Tests that UDT top-level inclusion overrides subfield specifications
      var json = "{\"" + udtCol() + "\":1, \"" + udtCol() + "." + cityField() + "\":1}";

      var fixture =
          tableWithUdt("inclusion UDT top level overrides sub level")
              .withProjectionJson(json)
              .applySelect()
              .assertSelectContains(udtCol());

      var out = fixture.projectRow();

      fixture.assertJsonHasObject(out, NAMES.CQL_NON_FROZEN_UDT_COLUMN_ADDRESS);
      var projectedUdt = out.get(NAMES.CQL_NON_FROZEN_UDT_COLUMN_ADDRESS.asInternal());

      assertThat(projectedUdt.get(cityField()).asText()).isEqualTo("default_" + cityField());
      assertThat(projectedUdt.get(countryField()).asText()).isEqualTo("default_" + countryField());
    }

    @Test
    public void exclusion_udt_top_level_removes_udt_from_selection() {
      // Tests that UDT top-level exclusion removes UDT from CQL SELECT
      var json = "{\"" + udtCol() + "\":0}";

      tableWithUdt("exclusion UDT top level removes UDT from selection")
          .withProjectionJson(json)
          .applySelect()
          .assertSelectNotContains(udtCol())
          .assertSelectContains(NAMES.CQL_INT_COLUMN.asInternal());
    }

    @Test
    public void exclusion_udt_sub_level_removes_specified_fields() {
      // Tests that UDT subfield exclusion removes only specified fields
      var json = "{\"" + udtCol() + "." + cityField() + "\":0}";

      var fixture =
          tableWithUdt("exclusion UDT sub level removes specified fields")
              .withProjectionJson(json)
              .applySelect()
              .assertSelectContains(udtCol());

      var out = fixture.projectRow();

      fixture.assertJsonHasObject(out, NAMES.CQL_NON_FROZEN_UDT_COLUMN_ADDRESS);
      var projectedUdt = out.get(NAMES.CQL_NON_FROZEN_UDT_COLUMN_ADDRESS.asInternal());
      assertThat(projectedUdt.has(cityField())).isFalse();
      assertThat(projectedUdt.get(countryField()).asText()).isEqualTo("default_" + countryField());
    }

    @Test
    public void exclusion_udt_all_subfields_removes_udt_from_selection() {
      // Tests that excluding all UDT subfields removes UDT from CQL SELECT
      var json =
          "{\""
              + udtCol()
              + "."
              + cityField()
              + "\":0, \""
              + udtCol()
              + "."
              + countryField()
              + "\":0}";

      tableWithUdt("exclusion UDT all subfields removes UDT from selection")
          .withProjectionJson(json)
          .applySelect()
          .assertSelectNotContains(udtCol())
          .assertSelectContains(NAMES.CQL_INT_COLUMN.asInternal());
    }

    /**
     * Sparse UDT fields (null) are not included in projection results even if explicitly selected.
     */
    @Test
    public void udt_null_fields_are_not_included_even_if_selected() {
      // Tests that null UDT fields are excluded from projection results even if selected
      var json =
          "{\""
              + udtCol()
              + "."
              + cityField()
              + "\":1, \""
              + udtCol()
              + "."
              + countryField()
              + "\":1}";

      var fixture =
          tableWithUdt("UDT null fields are not included even if selected")
              .withProjectionJson(json)
              .applySelect();

      // Create a UDT with null city field to test null handling
      var udtWithNullCity = createUdtValueWithNullCity("USA");
      var overrides = new HashMap<String, Object>();
      overrides.put(udtCol(), udtWithNullCity);
      var out = fixture.projectRowWithOverrides(overrides);

      fixture.assertJsonHasObject(out, NAMES.CQL_NON_FROZEN_UDT_COLUMN_ADDRESS);
      var projectedUdt = out.get(NAMES.CQL_NON_FROZEN_UDT_COLUMN_ADDRESS.asInternal());
      assertThat(projectedUdt.has(cityField())).isFalse();
      assertThat(projectedUdt.get(countryField()).asText()).isEqualTo("USA");
    }

    @Test
    public void udt_exclusion_projection_removes_specified_fields() {
      // Tests that UDT exclusion projection removes all specified subfields
      var json =
          "{\""
              + udtCol()
              + "."
              + cityField()
              + "\":0, \""
              + udtCol()
              + "."
              + countryField()
              + "\":0}";

      tableWithUdt("UDT exclusion projection removes specified fields")
          .withProjectionJson(json)
          .applySelect()
          .assertSelectNotContains("\"" + udtCol() + "\"")
          .assertSelectContains(NAMES.CQL_INT_COLUMN.asInternal());
    }

    @Test
    public void mixed_inclusion_exclusion_throws_error() {
      // Tests that mixing UDT inclusion and exclusion throws validation error
      var json =
          "{\""
              + udtCol()
              + "."
              + cityField()
              + "\":1, \""
              + udtCol()
              + "."
              + countryField()
              + "\":0}";
      assertThat(
              assertThrows(
                  RuntimeException.class,
                  () -> {
                    tableWithUdt("mixed inclusion/exclusion should throw error")
                        .withProjectionJson(json)
                        .applySelect();
                  }))
          .hasMessageContaining("cannot exclude");
    }
  }
}
