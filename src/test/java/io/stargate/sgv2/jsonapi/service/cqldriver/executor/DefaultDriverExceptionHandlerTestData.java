package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.stargate.sgv2.jsonapi.TestConstants;

public class DefaultDriverExceptionHandlerTestData {

  public final DriverExceptionHandler DRIVER_HANDLER;

  public TestConstants testConstants = new TestConstants();

  public final SimpleStatement STATEMENT =
      SimpleStatement.newInstance(
          "SELECT * FROM " + testConstants.TABLE_IDENTIFIER.table().asCql(true) + " WHERE x=?;", 1);

  public DefaultDriverExceptionHandlerTestData() {

    DRIVER_HANDLER =
        new DefaultDriverExceptionHandler<>(testConstants.TABLE_SCHEMA_OBJECT, STATEMENT);
  }
}
