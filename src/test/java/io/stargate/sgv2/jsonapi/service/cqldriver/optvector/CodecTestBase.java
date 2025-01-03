package io.stargate.sgv2.jsonapi.service.cqldriver.optvector;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.nio.ByteBuffer;

public class CodecTestBase<T> {
  protected TypeCodec<T> codec;

  protected String encode(T t, ProtocolVersion protocolVersion) {
    assertThat(codec).as("Must set codec before calling this method").isNotNull();
    ByteBuffer bytes = codec.encode(t, protocolVersion);
    return (bytes == null) ? null : Bytes.toHexString(bytes);
  }

  protected String encode(T t) {
    return encode(t, ProtocolVersion.DEFAULT);
  }

  protected T decode(String hexString, ProtocolVersion protocolVersion) {
    assertThat(codec).as("Must set codec before calling this method").isNotNull();
    ByteBuffer bytes = (hexString == null) ? null : Bytes.fromHexString(hexString);
    // Decode twice, to assert that decode leaves the input buffer in its original state
    codec.decode(bytes, protocolVersion);
    return codec.decode(bytes, protocolVersion);
  }

  protected T decode(String hexString) {
    return decode(hexString, ProtocolVersion.DEFAULT);
  }

  protected String format(T t) {
    assertThat(codec).as("Must set codec before calling this method").isNotNull();
    return codec.format(t);
  }

  protected T parse(String s) {
    assertThat(codec).as("Must set codec before calling this method").isNotNull();
    return codec.parse(s);
  }
}
