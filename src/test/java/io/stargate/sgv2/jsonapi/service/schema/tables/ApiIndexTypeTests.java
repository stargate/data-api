package io.stargate.sgv2.jsonapi.service.schema.tables;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.IndexKind;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlIndexException;
import java.util.Map;
import java.util.stream.Stream;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Test for the {@link ApiIndexType} enum. */
public class ApiIndexTypeTests {

  private static final ApiColumnDef INT_COL =
      new ApiColumnDef(CqlIdentifier.fromCql("INT_COL"), ApiDataTypeDefs.INT);
  private static final ApiColumnDef TEXT_COL =
      new ApiColumnDef(CqlIdentifier.fromCql("TEXT_COL"), ApiDataTypeDefs.TEXT);
  private static final ApiColumnDef ASCII_COL =
      new ApiColumnDef(CqlIdentifier.fromCql("ASCII_COL"), ApiDataTypeDefs.ASCII);
  private static final ApiColumnDef SET_COL =
      new ApiColumnDef(
          CqlIdentifier.fromCql("SET_COL"), new ApiSetType(ApiDataTypeDefs.TEXT, false));
  private static final ApiColumnDef LIST_COL =
      new ApiColumnDef(
          CqlIdentifier.fromCql("LIST_COL"), new ApiListType(ApiDataTypeDefs.TEXT, false));
  private static final ApiColumnDef MAP_COL =
      new ApiColumnDef(
          CqlIdentifier.fromCql("MAP_COL"),
          new ApiMapType(ApiDataTypeDefs.TEXT, ApiDataTypeDefs.TEXT, false));
  private static final ApiColumnDef VECTOR_COL =
      new ApiColumnDef(CqlIdentifier.fromCql("VECTOR_COL"), new ApiVectorType(512, null));

  @Test
  public void nullApiNameReturnsNull() {
    assertThat(ApiIndexType.fromApiName(null)).isNull();
  }

  @Test
  public void unknownApiNameReturnsNull() {
    assertThat(ApiIndexType.fromApiName("fake")).isNull();
  }

  @Test
  public void knownApiNameReturnsExpected() {
    assertThat(ApiIndexType.fromApiName(ApiIndexType.Constants.REGULAR))
        .isEqualTo(ApiIndexType.REGULAR);
  }

  @ParameterizedTest
  @MethodSource("supportedIndexTests")
  public void supportedIndex(
      ApiColumnDef columnDef, CQLSAIIndex.IndexTarget indexTarget, ApiIndexType expectedType) {

    var desc =
        "Expected a supported index type %s for column type %s with index target %s"
            .formatted(expectedType, columnDef.type().typeName(), indexTarget);
    ApiIndexType actualType = null;
    try {
      actualType = ApiIndexType.fromCql(columnDef, indexTarget, metadata());
    } catch (UnsupportedCqlIndexException e) {
      fail(desc, e);
    }

    assertThat(actualType).as(desc).isEqualTo(expectedType);
  }

  private static Stream<Arguments> supportedIndexTests() {
    return Stream.of(
        args(INT_COL, null, ApiIndexType.REGULAR),
        args(TEXT_COL, null, ApiIndexType.REGULAR),
        args(ASCII_COL, null, ApiIndexType.REGULAR),
        args(SET_COL, ApiIndexFunction.VALUES, ApiIndexType.REGULAR),
        args(LIST_COL, ApiIndexFunction.VALUES, ApiIndexType.REGULAR),
        args(MAP_COL, ApiIndexFunction.ENTRIES, ApiIndexType.REGULAR),
        args(VECTOR_COL, null, ApiIndexType.VECTOR));
  }

  private static Arguments args(
      ApiColumnDef columnDef, ApiIndexFunction indexFunction, ApiIndexType expectedType) {

    return Arguments.of(
        columnDef, new CQLSAIIndex.IndexTarget(columnDef.name(), indexFunction), expectedType);
  }

  @ParameterizedTest
  @MethodSource("unsupportedIndexTests")
  public void unsupportedIndex(ApiColumnDef columnDef, CQLSAIIndex.IndexTarget indexTarget) {

    var desc =
        "Expected unsupported index for column type %s with index target %s"
            .formatted(columnDef.type().typeName(), indexTarget);

    assertThrows(
        UnsupportedCqlIndexException.class,
        () -> ApiIndexType.fromCql(columnDef, indexTarget, metadata()),
        desc);
  }

  private static Stream<Arguments> unsupportedIndexTests() {
    return Stream.of(
        args(INT_COL, ApiIndexFunction.VALUES),
        args(TEXT_COL, ApiIndexFunction.VALUES),
        args(SET_COL, null),
        args(LIST_COL, null),
        args(MAP_COL, null),
        args(VECTOR_COL, ApiIndexFunction.VALUES, ApiIndexType.VECTOR));
  }

  private static Arguments args(ApiColumnDef columnDef, ApiIndexFunction indexFunction) {

    return Arguments.of(columnDef, new CQLSAIIndex.IndexTarget(columnDef.name(), indexFunction));
  }

  private static IndexMetadata metadata() {
    // index metadata is only used to get the index name when there is an error so we can use anon
    // object
    return new IndexMetadata() {
      @Override
      public @NotNull CqlIdentifier getKeyspace() {
        return null;
      }

      @Override
      public @NotNull CqlIdentifier getTable() {
        return null;
      }

      @Override
      public @NotNull CqlIdentifier getName() {
        return CqlIdentifier.fromInternal("testIndex");
      }

      @Override
      public @NotNull IndexKind getKind() {
        return null;
      }

      @Override
      public @NotNull String getTarget() {
        return "";
      }

      @Override
      public @NotNull Map<String, String> getOptions() {
        return Map.of();
      }
    };
  }
}
