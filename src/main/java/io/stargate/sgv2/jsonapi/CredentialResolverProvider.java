package io.stargate.sgv2.jsonapi;

import io.stargate.sgv2.jsonapi.api.request.CredentialResolver;
import io.stargate.sgv2.jsonapi.api.request.HeaderBasedCredentialResolver;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.Produces;

/** Simple CDI producer for the {@link CredentialResolver} to be used in the embedding service */
@Singleton
public class CredentialResolverProvider {
  @Inject HttpConstants httpConstants;

  @Produces
  @ApplicationScoped
  CredentialResolver headerTokenResolver() {
    return new HeaderBasedCredentialResolver(
        httpConstants.authenticationUserNameHeader(), httpConstants.authenticationPasswordHeader());
  }
}
