package io.stargate.sgv2.jsonapi.service.cqldriver;

import com.datastax.oss.driver.api.core.CqlSession;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Ticker;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Striped;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.cache.CaffeineCacheMetrics;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.stargate.sgv2.jsonapi.util.ClassUtils.classSimpleName;

/**
 * Object in Cache for QL Session Management
 */
public class CQLSessionCacheKey {
    private final String tenantId;
    private final CqlCredentials credentials;
    private final Duration ttl;
    @Nullable
    private final String userAgent;

    public CQLSessionCacheKey(
            String tenantId, CqlCredentials credentials, Duration ttl, @Nullable String userAgent) {
        if (tenantId == null || tenantId.isBlank()) {
            tenantId = "";
        }
        this.tenantId = tenantId;
        this.credentials = Objects.requireNonNull(credentials, "credentials must not be null");
        this.ttl = Objects.requireNonNull(ttl, "ttl must not be null");
        this.userAgent = userAgent;
    }

    public String tenantId() {
        return tenantId;
    }

    public CqlCredentials credentials() {
        return credentials;
    }

    public Duration ttl() {
        return ttl;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CQLSessionCacheKey)) return false;
        CQLSessionCacheKey that = (CQLSessionCacheKey) o;
        return tenantId.equals(that.tenantId) && credentials.equals(that.credentials);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tenantId, credentials);
    }

    @Override
    public String toString() {
        return new StringBuilder("SessionCacheKey{")
                .append("tenantId='").append(tenantId).append('\'')
                .append(", credentials=").append(credentials)
                .append(", ttl=").append(ttl)
                .append(", userAgent='").append(userAgent).append('\'')
                .append('}').toString();
    }
}