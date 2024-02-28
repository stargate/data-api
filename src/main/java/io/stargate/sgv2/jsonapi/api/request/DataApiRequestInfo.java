package io.stargate.sgv2.jsonapi.api.request;

import io.stargate.sgv2.api.common.tenant.TenantResolver;
import io.stargate.sgv2.api.common.token.CassandraTokenResolver;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.SecurityContext;
import java.util.Objects;
import java.util.Optional;

/**
 * This class is used to get the request info like tenantId and cassandraToken. This is a
 * replacement to DataApiRequestInfo so bridge connection is removed.
 */
@RequestScoped
public class DataApiRequestInfo {
  private Optional<String> tenantId;
  private final Optional<String> cassandraToken;

  private Optional<FileWriterParams> fileWriterParams;

  public DataApiRequestInfo() {
    this.cassandraToken = Optional.empty();
  }

  public DataApiRequestInfo(Optional<String> tenantId) {
    this.tenantId = tenantId;
    this.cassandraToken = Optional.empty();
  }

  @Inject
  public DataApiRequestInfo(
      RoutingContext routingContext,
      SecurityContext securityContext,
      Instance<TenantResolver> tenantResolver,
      Instance<CassandraTokenResolver> tokenResolver) {
    this.tenantId =
        ((TenantResolver) tenantResolver.get()).resolve(routingContext, securityContext);
    this.cassandraToken =
        ((CassandraTokenResolver) tokenResolver.get()).resolve(routingContext, securityContext);
    this.fileWriterParams = Optional.empty();
  }

  public void setTenantId(String tenantId) {
    this.tenantId = Optional.ofNullable(tenantId);
  }

  public Optional<String> getTenantId() {
    return this.tenantId;
  }

  public Optional<String> getCassandraToken() {
    return this.cassandraToken;
  }

  public Optional<FileWriterParams> getFileWriterParams() {
    return this.fileWriterParams;
  }

  public void setFileWriterParams(FileWriterParams fileWriterParams) {
    this.fileWriterParams = Optional.ofNullable(fileWriterParams);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DataApiRequestInfo that = (DataApiRequestInfo) o;
    return Objects.equals(tenantId, that.tenantId)
        && Objects.equals(cassandraToken, that.cassandraToken)
        && Objects.equals(fileWriterParams, that.fileWriterParams);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tenantId, cassandraToken, fileWriterParams);
  }
}
