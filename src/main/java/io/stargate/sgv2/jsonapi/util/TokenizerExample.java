package io.stargate.sgv2.jsonapi.util;

import ai.djl.huggingface.tokenizers.HuggingFaceTokenizer;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;

public class TokenizerExample {
  public static void main(String[] args) throws IOException {
    try (HuggingFaceTokenizer tokenizer =
        HuggingFaceTokenizer.newInstance(Paths.get("src/main/resources/tokenizer.json"))) {
      int totalToTest = 100_000;
      String[] prompts = new String[totalToTest];
      Arrays.fill(prompts, "Your text to tokenize here");
      long totalTime = 0;
      for (String prompt : prompts) {
        long start = System.nanoTime();
        int tokensCount = tokenizer.encode(prompt).getIds().length;
        if (tokensCount == 0) {
          throw new IllegalStateException("Token count should not be negative");
        }
        totalTime = totalTime + (System.nanoTime() - start);
      }
      System.out.println(
          "Total number of requests and average time taken for tokenization: "
              + prompts.length
              + " "
              + (totalTime / prompts.length)
              + " nanoseconds");
    }
  }
}
