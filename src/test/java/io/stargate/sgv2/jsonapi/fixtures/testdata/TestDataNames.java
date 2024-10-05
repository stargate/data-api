package io.stargate.sgv2.jsonapi.fixtures.testdata;

import com.datastax.oss.driver.api.core.CqlIdentifier;

public class TestDataNames {

  public final CqlIdentifier KEYSPACE_NAME =
      CqlIdentifier.fromInternal("keyspace-" + System.currentTimeMillis());

  public final CqlIdentifier TABLE_NAME =
      CqlIdentifier.fromInternal("table-" + System.currentTimeMillis());

  public final CqlIdentifier COL_PARTITION_KEY_1 =
      CqlIdentifier.fromInternal("partition-key-1-" + System.currentTimeMillis());
  public final CqlIdentifier COL_PARTITION_KEY_2 =
      CqlIdentifier.fromInternal("partition-key-2-" + System.currentTimeMillis());
  public final CqlIdentifier COL_CLUSTERING_KEY_1 =
      CqlIdentifier.fromInternal("clustering-key-1-" + System.currentTimeMillis());
  public final CqlIdentifier COL_CLUSTERING_KEY_2 =
      CqlIdentifier.fromInternal("clustering-key-2-" + System.currentTimeMillis());
  public final CqlIdentifier COL_CLUSTERING_KEY_3 =
      CqlIdentifier.fromInternal("clustering-key-3-" + System.currentTimeMillis());

  public final CqlIdentifier COL_REGULAR_KEY_1 =
      CqlIdentifier.fromInternal("regular-1-" + System.currentTimeMillis());
  public final CqlIdentifier COL_REGULAR_KEY_2 =
      CqlIdentifier.fromInternal("regular-2-" + System.currentTimeMillis());
  public final CqlIdentifier COL_REGULAR_KEY_3 =
      CqlIdentifier.fromInternal("regular-3-" + System.currentTimeMillis());
}
