package io.stargate.sgv2.jsonapi.testresource;

import io.quarkus.test.junit.QuarkusTestProfile;

/** Test profile that ignores global resources. */
public interface NoGlobalResourcesTestProfile extends QuarkusTestProfile {

  @Override
  default boolean disableGlobalTestResources() {
    return true;
  }

  /** Implementation of the {@link NoGlobalResourcesTestProfile} */
  class Impl implements NoGlobalResourcesTestProfile {}
}
