package io.stargate.sgv2.jsonapi.util;

public interface Recordable {

  DataRecorder recordTo(DataRecorder dataRecorder);

  default DataRecorder recordToSubRecorder(DataRecorder dataRecorder) {
    return recordTo(dataRecorder.beginSubRecorder(getClass())).endSubRecorder()
  }

}
