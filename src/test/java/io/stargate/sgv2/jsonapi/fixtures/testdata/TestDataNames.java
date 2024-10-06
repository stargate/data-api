package io.stargate.sgv2.jsonapi.fixtures.testdata;

import com.datastax.oss.driver.api.core.CqlIdentifier;

public class TestDataNames {

  public final CqlIdentifier KEYSPACE_NAME =
      CqlIdentifier.fromInternal("keyspace-" + System.currentTimeMillis());

  public final CqlIdentifier TABLE_NAME =
      CqlIdentifier.fromInternal("table-" + System.currentTimeMillis());
}
