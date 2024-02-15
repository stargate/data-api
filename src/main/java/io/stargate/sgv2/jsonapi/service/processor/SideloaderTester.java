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
    String writerSessionId = SideLoaderCommandProcessor.beginWriterSession(namespace, collection);
    System.out.println("Writer session id: " + writerSessionId);
    SSTableWriterStatus ssTableWriterStatus =
        SideLoaderCommandProcessor.insertDocuments(
            writerSessionId, List.of(new ObjectMapper().readTree("{\"id\": 1}")));
    System.out.println("SSTable writer status: " + ssTableWriterStatus);
    SSTableWriterStatus intermediateStatus =
        SideLoaderCommandProcessor.getWriterStatus(writerSessionId);
    System.out.println("Intermediate status: " + intermediateStatus);
    SideLoaderCommandProcessor.endWriterSession(writerSessionId);
  }
}
