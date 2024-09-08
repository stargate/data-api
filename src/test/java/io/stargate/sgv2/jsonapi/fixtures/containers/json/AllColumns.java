package io.stargate.sgv2.jsonapi.fixtures.containers.json;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import io.stargate.sgv2.jsonapi.fixtures.CqlFixture;

import java.util.ArrayList;
import java.util.List;

import static io.stargate.sgv2.jsonapi.fixtures.TestListUtil.difference;

/**
 * Generates one fixture for the table that includes all columns
 *
 * <p>Supported data types are checked, and added to the unsupported list
 */
public class AllColumns extends JsonContainerFixtureBuilder {

  public AllColumns(CqlFixture cqlFixture) {
    super(cqlFixture);
  }

  @Override
  protected List<JsonContainerFixture> getInternal(
      List<ColumnMetadata> allColumnsMetadata,
      List<ColumnMetadata> keysMetadata,
      List<ColumnMetadata> nonKeyMetadata) {
    List<JsonContainerFixture> fixtures = new ArrayList<>();

    var setKeysMetadata = keysMetadata;
    var missingKeysMetadata = difference(keysMetadata, setKeysMetadata);

    var setNonKeyMetadata = nonKeyMetadata;
    var missingNonKeyMetadata = difference(nonKeyMetadata, setNonKeyMetadata);

    fixtures.add(
        fixture(
            setKeysMetadata,
            setNonKeyMetadata,
            missingKeysMetadata,
            missingNonKeyMetadata,
            List.of()
    ));
    return fixtures;
  }
}
