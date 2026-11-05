/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.stargate.sgv2.jsonapi.api.security.configuration;

import io.quarkus.arc.lookup.LookupIfProperty;
import io.quarkus.runtime.Startup;
import io.quarkus.security.identity.IdentityProvider;
import io.quarkus.vertx.http.runtime.security.HttpAuthenticationMechanism;
import io.smallrye.jwt.auth.principal.JWTAuthContextInfo;
import io.stargate.sgv2.jsonapi.api.security.HeaderAuthenticationRequest;
import io.stargate.sgv2.jsonapi.api.security.HeaderBasedAuthenticationMechanism;
import io.stargate.sgv2.jsonapi.api.security.HeaderIdentityProvider;
import io.stargate.sgv2.jsonapi.api.security.McpOAuthMetadata;
import io.stargate.sgv2.jsonapi.api.security.challenge.impl.ErrorChallengeSender;
import io.stargate.sgv2.jsonapi.config.AuthConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import java.util.ArrayList;

/** The configuration for the header based security. */
public class HeaderBasedSecurityConfiguration {

  @Produces
  @ApplicationScoped
  @Startup
  HttpAuthenticationMechanism httpAuthenticationMechanism(
      AuthConfig config,
      Instance<ErrorChallengeSender> customChallengeSender,
      McpOAuthMetadata metadata,
      JWTAuthContextInfo jwtConfig) {
    if (config.bearer().enabled()) {
      validateBearerConfiguration(jwtConfig);
    }
    return new HeaderBasedAuthenticationMechanism(config, customChallengeSender, metadata);
  }

  static void validateBearerConfiguration(JWTAuthContextInfo config) {
    var missing = new ArrayList<String>();
    if (!isConfigured(config.getIssuedBy())) {
      missing.add("mp.jwt.verify.issuer");
    }
    if (!isConfigured(config.getPublicKeyLocation())
        && !isConfigured(config.getPublicKeyContent())
        && config.getPublicVerificationKey() == null) {
      missing.add("mp.jwt.verify.publickey.location or mp.jwt.verify.publickey");
    }
    if (config.getExpectedAudience() == null
        || config.getExpectedAudience().stream()
            .noneMatch(HeaderBasedSecurityConfiguration::isConfigured)) {
      missing.add("mp.jwt.verify.audiences");
    }
    if (!missing.isEmpty()) {
      throw new IllegalStateException(
          "Bearer authentication requires " + String.join(", ", missing));
    }
  }

  private static boolean isConfigured(String value) {
    return value != null && !value.isBlank();
  }

  @Produces
  @ApplicationScoped
  @LookupIfProperty(name = "stargate.auth.header-based.enabled", stringValue = "true")
  IdentityProvider<HeaderAuthenticationRequest> identityProvider() {
    return new HeaderIdentityProvider();
  }
}
