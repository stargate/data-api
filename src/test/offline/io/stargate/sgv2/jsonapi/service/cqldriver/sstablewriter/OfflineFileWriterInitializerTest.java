package io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.stargate.sgv2.jsonapi.config.constants.ApiConstants;
import org.junit.jupiter.api.Test;

public class OfflineFileWriterInitializerTest {

  @Test
  public void testInitialize() {
    OfflineFileWriterInitializer.initialize();
    assertEquals("true", System.getProperty(ApiConstants.OFFLINE_WRITER_MODE_PROPERTY));
  }
}
