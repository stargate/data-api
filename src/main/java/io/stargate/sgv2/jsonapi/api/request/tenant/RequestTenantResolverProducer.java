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

package io.stargate.sgv2.jsonapi.api.request.tenant;

import io.quarkus.arc.lookup.LookupIfProperty;
import io.stargate.sgv2.jsonapi.config.MultiTenancyConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

/** Configuration for activating a correct {@link RequestTenantResolver}. */
public class RequestTenantResolverProducer {

  @Produces
  @ApplicationScoped
  @LookupIfProperty(name = "stargate.multi-tenancy.tenant-resolver.type", stringValue = "subdomain")
  @LookupIfProperty(name = "stargate.multi-tenancy.enabled", stringValue = "true")
  RequestTenantResolver subdomainTenantResolver(MultiTenancyConfig config) {
    return new SubdomainTenantResolver(config.tenantResolver().subdomain());
  }

  @Produces
  @ApplicationScoped
  @LookupIfProperty(name = "stargate.multi-tenancy.tenant-resolver.type", stringValue = "fixed")
  @LookupIfProperty(name = "stargate.multi-tenancy.enabled", stringValue = "true")
  RequestTenantResolver fixedTenantResolver(MultiTenancyConfig config) {
    return new FixedTenantResolver(config.tenantResolver().fixed().tenantId().orElse(""));
  }

  @Produces
  @ApplicationScoped
  @LookupIfProperty(
      name = "stargate.multi-tenancy.enabled",
      stringValue = "false",
      lookupIfMissing = true)
  RequestTenantResolver noopTenantResolver() {
    var none = TenantFactory.instance().create(null);

    return (context, securityContext) -> none;
  }
}
