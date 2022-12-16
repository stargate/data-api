package io.stargate.sgv3.docsapi.service.shredding;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import javax.inject.Inject;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class DocValueHasherTest {
  @Inject ObjectMapper objectMapper;

  public void testSimpleDocument() throws Exception {}

  public void testNestedArray() throws Exception {}

  public void testNestedObject() throws Exception {}
}
