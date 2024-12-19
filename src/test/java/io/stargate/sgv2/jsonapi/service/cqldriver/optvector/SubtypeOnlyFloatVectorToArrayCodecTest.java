/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.sgv2.jsonapi.service.cqldriver.optvector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.DefaultVectorType;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.optvector.SubtypeOnlyFloatVectorToArrayCodec;
import org.junit.Test;

/**
 * Basic sanity checks to make sure {@link SubtypeOnlyFloatVectorToArrayCodec} is a wall-behaved
 * type codec
 */
public class SubtypeOnlyFloatVectorToArrayCodecTest extends CodecTestBase<float[]> {

  private static final float[] VECTOR = new float[] {1.0f, 2.5f};

  private static final String VECTOR_HEX_STRING = "0x" + "3f800000" + "40200000";

  private static final String FORMATTED_VECTOR = "[1.0, 2.5]";

  public SubtypeOnlyFloatVectorToArrayCodecTest() {
    codec = SubtypeOnlyFloatVectorToArrayCodec.instance();
  }

  @Test
  public void should_encode() {
    assertThat(encode(VECTOR)).isEqualTo(VECTOR_HEX_STRING);
    assertThat(encode(null)).isNull();
  }

  @Test
  public void should_decode() {
    assertThat(decode(VECTOR_HEX_STRING)).isEqualTo(VECTOR);
    assertThatThrownBy(() -> decode("0x")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> decode(null)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void should_throw_on_decode_if_too_few_bytes() {
    // Dropping 4 bytes would knock off exactly 1 float, anything less than that would be something
    // we couldn't parse a float out of
    for (int i = 1; i <= 3; ++i) {
      // 2 chars of hex encoded string = 1 byte
      int lastIndex = VECTOR_HEX_STRING.length() - (2 * i);
      assertThatThrownBy(() -> decode(VECTOR_HEX_STRING.substring(0, lastIndex)))
          .isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Test
  public void should_format() {
    assertThat(format(VECTOR)).isEqualTo(FORMATTED_VECTOR);
    assertThat(format(null)).isEqualTo("NULL");
  }

  @Test
  public void should_parse() {
    assertThat(parse(FORMATTED_VECTOR)).isEqualTo(VECTOR);
    assertThatThrownBy(() -> parse("NULL")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> parse("null")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> parse("")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> parse(null)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void should_accept_data_type() {
    assertThat(codec.accepts(DataTypes.vectorOf(DataTypes.FLOAT, 2))).isTrue();
    assertThat(codec.accepts(DataTypes.INT)).isFalse();
  }

  @Test
  public void should_accept_vector_type_all_dimension_only() {
    for (int i = 0; i < 1000; ++i) {
      assertThat(codec.accepts(new DefaultVectorType(DataTypes.FLOAT, i))).isTrue();
    }
  }

  @Test
  public void should_accept_generic_type() {
    assertThat(codec.accepts(GenericType.of(float[].class))).isTrue();
    assertThat(codec.accepts(GenericType.arrayOf(Float.class))).isFalse();
    assertThat(codec.accepts(GenericType.arrayOf(Integer.class))).isFalse();
    assertThat(codec.accepts(GenericType.of(Float.class))).isFalse();
    assertThat(codec.accepts(GenericType.of(Integer.class))).isFalse();
  }

  @Test
  public void should_accept_raw_type() {
    assertThat(codec.accepts(float[].class)).isTrue();
    assertThat(codec.accepts(Integer.class)).isFalse();
  }

  @Test
  public void should_accept_object() {
    assertThat(codec.accepts(VECTOR)).isTrue();
    assertThat(codec.accepts(Integer.MIN_VALUE)).isFalse();
  }
}
