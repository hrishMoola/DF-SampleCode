package edu.usfca.dataflow.transforms;

import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multiset;
import edu.usfca.dataflow.utils.ProtoUtils;
import edu.usfca.protobuf.Profile;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions.CheckEnabled;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static edu.usfca.dataflow.transforms.__Utils.getCanonicalDeviceId;
import static edu.usfca.dataflow.transforms.__Utils.getMultiSet;
import static org.junit.Assert.assertEquals;

public class __TestsWith01TinyDataset {
  // Grading system will use this timeout to make sure it does not run indefinitely.
  // 20 seconds per test should be more than sufficient, FYI.
  // You can disable this for your local tests, though.
  @Rule
  public Timeout timeout = Timeout.millis(100000000);

  @Rule
  public final transient TestPipeline tp = TestPipeline.create();

  @Before
  public void before() {
    tp.getOptions().setStableUniqueNames(CheckEnabled.OFF);
  }

  // NOTE: shareable tests & hidden test(s) will be use this file.
  final static String PATH_TO_FILE = "../judge/resources/sample-tiny.txt";
//  final static String PATH_TO_FILE = "../dataflow/resources/chuma";
  @Test
  public void __shareable__01tiny_02HighSpender() {
    PCollection<Profile.PurchaserProfile> actualMerged =
            tp.apply(TextIO.read().from(PATH_TO_FILE)).apply(new PurchaserProfiles.GetProfilesFromEvents()).apply(new PurchaserProfiles.MergeProfiles());

    {
      Multiset<String> expectedIds = getMultiSet("CAESJDMwRUVERDE4LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
              "CAESJEJEREQwOUYwLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
              "CAESJENCQTI1Q0MyLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==");
      PAssert
              .that(actualMerged
                      .apply(new ExtractData.ExtractHighSpenders(1, 1L, new ImmutableSet.Builder<String>().add("id686486").build())))
              .satisfies(out -> {

                ImmutableMultiset<String> actualIds = new ImmutableMultiset.Builder<String>()
                        .addAll(StreamSupport.stream(out.spliterator(), false)
                                .map(id -> ProtoUtils.encodeMessageBase64(getCanonicalDeviceId(id))).collect(Collectors.toList()))
                        .build();

                assertEquals(expectedIds, actualIds);
                return null;
              });
    }

    {
      Multiset<String> expectedIds = getMultiSet("CAESJENCQTI1Q0MyLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==");
      PAssert
              .that(actualMerged
                      .apply(new ExtractData.ExtractHighSpenders(1, 300L, new ImmutableSet.Builder<String>().add("id686486").build())))
              .satisfies(out -> {

                ImmutableMultiset actualIds = new ImmutableMultiset.Builder<String>()
                        .addAll(StreamSupport.stream(out.spliterator(), false)
                                .map(id -> ProtoUtils.encodeMessageBase64(getCanonicalDeviceId(id))).collect(Collectors.toList()))
                        .build();

                assertEquals(expectedIds, actualIds);
                return null;
              });
    }

    {
      PAssert.that(
              actualMerged.apply(new ExtractData.ExtractHighSpenders(1, 1000L, new ImmutableSet.Builder<String>().add("김떡순").build())))
              .empty();
    }

    tp.run();
  }

}
