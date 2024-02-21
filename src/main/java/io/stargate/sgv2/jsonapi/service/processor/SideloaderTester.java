package io.stargate.sgv2.jsonapi.service.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class SideloaderTester {
  public static void main(String[] args)
      throws JsonProcessingException, ExecutionException, InterruptedException {
    String namespace = "demo_namespace";
    String collection = "players";

    // 1
    String writerSessionId = SideLoaderCommandProcessor.beginWriterSession(namespace, collection);

    System.out.println("Writer session id: " + writerSessionId);

    // 2
    SSTableWriterStatus ssTableWriterStatus =
        SideLoaderCommandProcessor.insertDocuments(
            writerSessionId, List.of(new ObjectMapper().readTree("{\"id\": 1}")));

    System.out.println("SSTable writer status: " + ssTableWriterStatus);

    // 3
    SSTableWriterStatus intermediateStatus =
        SideLoaderCommandProcessor.getWriterStatus(writerSessionId);
    System.out.println("Intermediate status: " + intermediateStatus);

    // 4
    SideLoaderCommandProcessor.endWriterSession(writerSessionId);
  }
}
