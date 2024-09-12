package io.stargate.sgv2.jsonapi.fixtures.containers.json;

import static io.stargate.sgv2.jsonapi.fixtures.TestListUtil.difference;
import static io.stargate.sgv2.jsonapi.fixtures.TestListUtil.testCombinations;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import io.stargate.sgv2.jsonapi.fixtures.CqlFixture;
import java.util.ArrayList;
import java.util.List;

/** Generates one fixture for each missing primary key */
public class MissingPrimaryKeys extends JsonContainerFixtureBuilder {

  public MissingPrimaryKeys(CqlFixture cqlFixture) {
    super(cqlFixture);
  }

  @Override
  protected List<JsonContainerFixture> getInternal(
      List<ColumnMetadata> allColumnsMetadata,
      List<ColumnMetadata> keysMetadata,
      List<ColumnMetadata> nonKeyMetadata) {
    List<JsonContainerFixture> fixtures = new ArrayList<>();

    var setNonKeyMetadata = nonKeyMetadata;
    var missingNonKeyMetadata = difference(nonKeyMetadata, setNonKeyMetadata);

    testCombinations(keysMetadata, true, false)
        .forEach(
            combination -> {
              var setKeysMetadata = combination;
              var missingKeysMetadata = difference(keysMetadata, setKeysMetadata);

              fixtures.add(
                  fixture(
                      setKeysMetadata,
                      setNonKeyMetadata,
                      missingKeysMetadata,
                      missingNonKeyMetadata,
                      List.of()));
            });
    return fixtures;
  }
}
