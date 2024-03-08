package io.stargate.sgv2.jsonapi.grpc.retries.impl;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.ProtoUtils;
import io.stargate.bridge.proto.QueryOuterClass;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class JsonApiGrpcRetryPolicyTest {

  JsonApiGrpcRetryPolicy policy = new JsonApiGrpcRetryPolicy();

  @Nested
  class PredicateTest {

    @Test
    public void unavailable() {
      StatusRuntimeException e = new StatusRuntimeException(Status.UNAVAILABLE);

      boolean result = policy.test(e);

      assertThat(result).isTrue();
    }

    @Test
    public void deadlineWithReadTimeout() {
      Metadata.Key<QueryOuterClass.ReadTimeout> key =
          ProtoUtils.keyForProto(QueryOuterClass.ReadTimeout.getDefaultInstance());
      QueryOuterClass.ReadTimeout value = QueryOuterClass.ReadTimeout.newBuilder().build();
      Metadata metadata = new Metadata();
      metadata.put(key, value);
      StatusRuntimeException e = new StatusRuntimeException(Status.DEADLINE_EXCEEDED, metadata);

      boolean result = policy.test(e);

      assertThat(result).isTrue();
    }

    @Test
    public void deadlineWithWriteTimeout() {
      Metadata.Key<QueryOuterClass.WriteTimeout> key =
          ProtoUtils.keyForProto(QueryOuterClass.WriteTimeout.getDefaultInstance());
      QueryOuterClass.WriteTimeout value = QueryOuterClass.WriteTimeout.newBuilder().build();
      Metadata metadata = new Metadata();
      metadata.put(key, value);
      StatusRuntimeException e = new StatusRuntimeException(Status.DEADLINE_EXCEEDED, metadata);

      boolean result = policy.test(e);

      assertThat(result).isTrue();
    }

    @Test
    public void deadlineWithWrongTrailer() {
      Metadata.Key<QueryOuterClass.Unavailable> key =
          ProtoUtils.keyForProto(QueryOuterClass.Unavailable.getDefaultInstance());
      QueryOuterClass.Unavailable value = QueryOuterClass.Unavailable.newBuilder().build();
      Metadata metadata = new Metadata();
      metadata.put(key, value);
      StatusRuntimeException e = new StatusRuntimeException(Status.DEADLINE_EXCEEDED, metadata);

      boolean result = policy.test(e);

      assertThat(result).isFalse();
    }

    @Test
    public void deadlineWithoutTrailer() {
      StatusRuntimeException e = new StatusRuntimeException(Status.DEADLINE_EXCEEDED);

      boolean result = policy.test(e);

      assertThat(result).isFalse();
    }

    @Test
    public void ignoredStatusCode() {
      StatusRuntimeException e = new StatusRuntimeException(Status.INTERNAL);

      boolean result = policy.test(e);

      assertThat(result).isFalse();
    }
  }
}
