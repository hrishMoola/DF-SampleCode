package edu.usfca.dataflow.transforms;

import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multiset;
import com.google.protobuf.InvalidProtocolBufferException;
import edu.usfca.dataflow.utils.ProtoUtils;
import edu.usfca.protobuf.Common;
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


public class __TestsWith03MediumDataset {
  // Grading system will use this timeout to make sure it does not run indefinitely.
  // 20 seconds per test should be more than sufficient, FYI.
  // You can disable this for your local tests, though.
  @Rule
  public Timeout timeout = Timeout.millis(400000000);

  @Rule
  public final transient TestPipeline tp = TestPipeline.create();

  @Before
  public void before() {
    tp.getOptions().setStableUniqueNames(CheckEnabled.OFF);
  }

  // NOTE: shareable tests & hidden test(s) will be use this file.
  final static String PATH_TO_FILE = "../judge/resources/sample-medium.txt";

  @Test
  public void __shareable__03medium_02HighSpender() {
    PCollection<Profile.PurchaserProfile> actualMerged =
            tp.apply(TextIO.read().from(PATH_TO_FILE)).apply(new PurchaserProfiles.GetProfilesFromEvents()).apply(new PurchaserProfiles.MergeProfiles());

    {
      Multiset<String> expectedIds = getMultiSet("CAESJDM2OUVCNjNFLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
              "CAESJEY2MTJGQkZELVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
              "CAESJEE4OTZEREU4LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
              "CAESJDAwQjYxNkY4LVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==");

//      expectedIds.forEach(id ->{
//        try {
//          System.out.println(ProtoUtils.decodeMessageBase64(Common.DeviceId.parser(),id));
//        } catch (InvalidProtocolBufferException e) {
//          e.printStackTrace();
//        }
//      });


      PAssert
              .that(actualMerged
                      .apply(new ExtractData.ExtractHighSpenders(4, 1000L, new ImmutableSet.Builder<String>().add("id486").build())))
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
      Multiset<String> expectedIds = getMultiSet("CAESJDVGNEEzN0JBLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==",
              "CAISJDAwMDAwMDAwLVhYWFgtTVhYWC1OWFhYLVhYWFhYWFhYWFhYWA==");
            expectedIds.forEach(id ->{
        try {
          System.out.println(ProtoUtils.decodeMessageBase64(Common.DeviceId.parser(),id));
        } catch (InvalidProtocolBufferException e) {
          e.printStackTrace();
        }
      });
      PAssert
              .that(actualMerged.apply(new ExtractData.ExtractHighSpenders(5, 12345L,
                      new ImmutableSet.Builder<String>()
                              .add("id686", "id486", "id486686", "id686486", "this", "does", "not", "exist").build())))
              .satisfies(out -> {

                ImmutableMultiset<String> actualIds = new ImmutableMultiset.Builder<String>()
                        .addAll(StreamSupport.stream(out.spliterator(), false)
                                .map(id -> ProtoUtils.encodeMessageBase64(getCanonicalDeviceId(id))).collect(Collectors.toList()))
                        .build();

                assertEquals(expectedIds, actualIds);
                return null;
              });
    }

    tp.run();
  }

}
