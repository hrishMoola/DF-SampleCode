package edu.usfca.dataflow.transforms;

import org.apache.beam.sdk.options.PipelineOptions.CheckEnabled;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

public class __TestsWith04LargeDataset {
  // Grading system will use this timeout to make sure it does not run indefinitely.
  // 20 seconds per test should be more than sufficient, FYI.
  // You can disable this for your local tests, though.
  @Rule
  public Timeout timeout = Timeout.millis(100000);

  @Rule
  public final transient TestPipeline tp = TestPipeline.create();

  @Before
  public void before() {
    tp.getOptions().setStableUniqueNames(CheckEnabled.OFF);
  }

  // NOTE: shareable tests & hidden test(s) will be use this file.
  final static String PATH_TO_FILE = "../judge/resources/sample-large.txt";

  @Test
  public void test() {
    // this is a dummy test. freebies!
  }
}
