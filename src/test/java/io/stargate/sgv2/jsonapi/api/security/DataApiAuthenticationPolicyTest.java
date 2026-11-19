package io.stargate.sgv2.jsonapi.api.security;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.security.StringPermission;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.runtime.QuarkusSecurityIdentity;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;

class DataApiAuthenticationPolicyTest {
  @Test
  void acceptsTheDataApiIdentityWithItsExistingPermissions() {
    var read = new StringPermission("read");
    var identity =
        QuarkusSecurityIdentity.builder().setPrincipal(() -> "user").addPermission(read).build();

    var authenticated = DataApiAuthenticationPolicy.authenticatedIdentity(identity);
    assertThat(isPermitted(authenticated)).isTrue();
    assertThat(authenticated.getPermissions()).containsExactly(read);
  }

  @Test
  void leavesAnonymousAccessToTheRoutePermission() {
    var identity = QuarkusSecurityIdentity.builder().setAnonymous(true).build();
    assertThat(isPermitted(identity)).isTrue();
  }

  @Test
  void rejectsAnIdentityFromAnotherMechanism() {
    var identity = QuarkusSecurityIdentity.builder().setPrincipal(() -> "other-user").build();
    assertThat(isPermitted(identity)).isFalse();
  }

  private static boolean isPermitted(SecurityIdentity identity) {
    return new DataApiAuthenticationPolicy()
        .checkPermission(null, Uni.createFrom().item(identity), null)
        .await()
        .indefinitely()
        .isPermitted();
  }
}
