package io.stargate.sgv2.jsonapi.service.cqldriver.optvector;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.MutableCodecRegistry;
import com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.optvector.SubtypeOnlyFloatVectorToArrayCodec;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

/**
 * Test of the full suite of "subtype only" functionality. Goal here is to confirm two distinct
 * questions:
 *
 * <p>* If we use the "subtype only" type with a {@link DefaultCodecRegistry} do we get the same
 * codec regardless of vector dimension? * Can we use the codec we get back from the default codec
 * registry to encode and decode vectors of different sizes?
 *
 * <p>Note that all of this works only because of an implementation detail in DefaultCodecRegistry.
 * The use of Objects.equals() in the code referenced below means that we effectively use the
 * equals() method of the DataType impl to determine whether keys in the codec cache match. We
 * leverage this behaviour to make SubtypeOnlyVectorType match <i>all</i> vectors with an equivalent
 * subtype. This behaviour is thus not guaranteed for other codec registry impls.
 *
 * <p><a
 * href="https://github.com/apache/cassandra-java-driver/blob/4.18.1/core/src/main/java/com/datastax/oss/driver/internal/core/type/codec/registry/DefaultCodecRegistry.java#L152-L153">Codec
 * registry code</a>
 */
public class SubtypeOnlyFloatVectorTest {

  @Test
  public void shouldFindSubtypeOnlyCodecRegardlessOfSize() {

    MutableCodecRegistry registry = new DefaultCodecRegistry("subtype_only");
    registry.register(SubtypeOnlyFloatVectorToArrayCodec.instance());

    AtomicReference<TypeCodec<float[]>> codecRef = new AtomicReference<>();
    for (int i = 1; i <= 2000; ++i) {

      TypeCodec<float[]> codec = registry.codecFor(DataTypes.vectorOf(DataTypes.FLOAT, i));
      codecRef.compareAndSet(null, codec);
      assertThat(codec).isInstanceOf(SubtypeOnlyFloatVectorToArrayCodec.class);
      assertThat(codec).isEqualTo(codecRef.get());
    }
  }

  @Test
  public void shouldEncodeAndDecodeVectorsOfArbitrarySize() {

    MutableCodecRegistry registry = new DefaultCodecRegistry("subtype_only");
    registry.register(SubtypeOnlyFloatVectorToArrayCodec.instance());

    for (int i = 1; i <= 2000; ++i) {

      TypeCodec<float[]> codec = registry.codecFor(DataTypes.vectorOf(DataTypes.FLOAT, i));
      float[] comparison = randomFloatArray(i);
      float[] result =
          codec.decode(codec.encode(comparison, ProtocolVersion.V4), ProtocolVersion.V4);
      assertThat(result).isEqualTo(comparison);
    }
  }

  private float[] randomFloatArray(int size) {
    // Use fixed seed for reproducibility
    Random random = new Random(size);
    float[] rv = new float[size];
    for (int i = 0; i < size; ++i) {
      rv[0] = random.nextFloat();
    }
    return rv;
  }
}
