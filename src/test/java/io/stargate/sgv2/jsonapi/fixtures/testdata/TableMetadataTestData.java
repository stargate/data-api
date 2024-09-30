package io.stargate.sgv2.jsonapi.fixtures.testdata;

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultTableMetadata;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class TableMetadataTestData extends TestDataSuplier {

  public TableMetadataTestData(TestData testData) {
    super(testData);
  }

  public TableMetadata empty() {
    return new DefaultTableMetadata(
        names.KEYSPACE_NAME,
        names.TABLE_NAME,
        UUID.randomUUID(),
        false,
        false,
        List.of(),
        Map.of(),
        Map.of(),
        Map.of(),
        Map.of());
  }
}
