package edu.usfca.dataflow.transforms;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.beam.sdk.options.PipelineOptions.CheckEnabled;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import edu.usfca.dataflow.transforms.ExtractData.ExtractAddicts;
import edu.usfca.dataflow.transforms.ExtractData.ExtractHighSpenders;
import edu.usfca.dataflow.transforms.ExtractData.ExtractInAppPurchaseData;
import edu.usfca.dataflow.transforms.PurchaserProfiles.GetProfilesFromEvents;
import edu.usfca.dataflow.transforms.PurchaserProfiles.MergeProfiles;
import edu.usfca.protobuf.Common.DeviceId;
import edu.usfca.protobuf.Common.OsType;
import edu.usfca.protobuf.Profile.InAppPurchaseProfile;
import edu.usfca.protobuf.Profile.PurchaserProfile;

public class __TestExtractData {
  // Grading system will use this timeout to make sure it does not run indefinitely.
  // The timeout provided below should be more than sufficient (yet, if it turns out it's too short, I'll adjust it and
  // re-grade so you don't have to worry about that).
  // You can disable this for your local tests, though (just remove the following two lines).
  @Rule
  public Timeout timeout = Timeout.millis(5000);

  @Rule
  public final transient TestPipeline tp = TestPipeline.create();

  @Before
  public void before() {
    tp.getOptions().setStableUniqueNames(CheckEnabled.OFF);
  }

  static final String[] SAMPLES = new String[] {
      "{\"httpRequest\":{\"remoteIp\":\"81.153.19.87\",\"requestMethod\":\"GET\",\"status\":200,\"serverIp\":\"127.0.0.1\",\"requestUrl\":\"https://www.cs.usfca.edu/you/are/awesome?ios_idfa=F199DFDE-xxxx-Mxxx-Nxxx-xxxxxxxxxxxx&amount=1869.919&event_type=PURChaSE&event_id=Xw3rY5&store=AppsTorE&bundle=F7GEqu1ZIlMthlv25rjvimSc\"},\"insertId\":\"WHTy7S3p8\",\"logName\":\"projects/beer-spear/logs/requests\",\"timestamp\":\"2020-02-20T01:53:59.715888Z\",\"spanId\":\"wiYcjQj5BqFG\",\"trace\":\"projects/beer-spear/traces/IkUh223zqzTFjms\"}",
      "{\"httpRequest\":{\"remoteIp\":\"56.32.201.235\",\"requestMethod\":\"GET\",\"status\":200,\"serverIp\":\"127.0.0.1\",\"requestUrl\":\"https://www.cs.usfca.edu/you/are/awesome?amount=7235.772&event_type=puRcHaSe&event_id=80ki4wQsfkd65h&store=apPSTorE&gps_adid=5eef43d1-xxxx-Mxxx-Nxxx-xxxxxxxxxxxx&bundle=01Xr27jPKgUqs\"},\"insertId\":\"eKeonckQUVU4\",\"logName\":\"projects/beer-spear/logs/requests\",\"timestamp\":\"2020-02-20T01:56:21.012462Z\",\"spanId\":\"33xxPPVbUSj9OQXF92\",\"trace\":\"projects/beer-spear/traces/YqBzFC6Zr4U62xkOAoFIM\"}",
      "{\"httpRequest\":{\"remoteIp\":\"106.121.56.150\",\"requestMethod\":\"GET\",\"status\":200,\"serverIp\":\"127.0.0.1\",\"requestUrl\":\"https://www.cs.usfca.edu/you/are/awesome?minecraft=pretty+fun&amount=2389.431&event_type=PURcHASe&event_id=425W8DnokBCgtqe4K&store=AppsToRe&gps_adid=54e94a77-xxxx-Mxxx-Nxxx-xxxxxxxxxxxx&bundle=ZtPW19&pubg=never+played\"},\"insertId\":\"H2OUVmwvdnY\",\"logName\":\"projects/beer-spear/logs/requests\",\"timestamp\":\"2020-02-20T01:56:11.14286Z\",\"spanId\":\"h9qpv3EEYk1UXNms1d\",\"trace\":\"projects/beer-spear/traces/42UZdwb2g941zOhVdjQ\"}",
      "{\"httpRequest\":{\"remoteIp\":\"9.95.219.68\",\"requestMethod\":\"GET\",\"status\":200,\"serverIp\":\"127.0.0.1\",\"requestUrl\":\"https://www.cs.usfca.edu/you/are/awesome?amount=243.089&event_type=PurCHaSE&event_id=TU0scIh5FN&store=appS_ore&gps_adid=bddd09f0-xxxx-Mxxx-Nxxx-xxxxxxxxxxxx&bundle=4lPdMCgd\"},\"insertId\":\"CSdoRkGVI3rl\",\"logName\":\"projects/beer-spear/logs/requests\",\"timestamp\":\"2020-02-20T01:56:25.2928Z\",\"spanId\":\"UtAQIZ75NoC602Ew\",\"trace\":\"projects/beer-spear/traces/J0gEXBHVi4Ptq8VW3oiqdw\"}"};

  static GetProfilesFromEvents PTransformGetPP = new GetProfilesFromEvents();

  static MergeProfiles PTransformMerge = new MergeProfiles();

  static ExtractInAppPurchaseData PTransformIAP = new ExtractInAppPurchaseData();

  static ExtractAddicts PTransformAddicts1 = new ExtractAddicts("ZtPW19", 1);
  static ExtractAddicts PTransformAddicts2 = new ExtractAddicts("ZtPW19", 2);

  @Test
  public void testIAP() {
    PCollection<PurchaserProfile> pp =
        tp.apply(Create.of(Arrays.asList(SAMPLES))).apply(PTransformGetPP).apply(PTransformMerge);

    List<InAppPurchaseProfile> expected = Arrays.asList(
        InAppPurchaseProfile.newBuilder().setBundle("01Xr27jPKgUqs").setNumPurchasers(1L).setTotalAmount(7236L).build(),
        InAppPurchaseProfile.newBuilder().setBundle("4lPdMCgd").setNumPurchasers(1L).setTotalAmount(243L).build(),
        InAppPurchaseProfile.newBuilder().setBundle("ZtPW19").setNumPurchasers(1L).setTotalAmount(2389L).build(),
        InAppPurchaseProfile.newBuilder().setBundle("F7GEqu1ZIlMthlv25rjvimSc").setNumPurchasers(1L)
            .setTotalAmount(1870L).build());

    PAssert.that(pp.apply(PTransformIAP)).containsInAnyOrder(expected);

    tp.run();
  }

  @Test
  public void testHigh() {
    PCollection<PurchaserProfile> pp =
        tp.apply(Create.of(Arrays.asList(SAMPLES))).apply(PTransformGetPP).apply(PTransformMerge);

    Set<String> bundles = Sets.newHashSet("01Xr27jPKgUqs", "4lPdMCgd", "ZtPW19", "커피");

    {
      Set<DeviceId> expected = Sets.newHashSet(
          DeviceId.newBuilder().setOs(OsType.ANDROID).setUuid("BDDD09F0-XXXX-MXXX-NXXX-XXXXXXXXXXXX").build(),
          DeviceId.newBuilder().setOs(OsType.ANDROID).setUuid("5EEF43D1-XXXX-MXXX-NXXX-XXXXXXXXXXXX").build(),
          DeviceId.newBuilder().setOs(OsType.ANDROID).setUuid("54E94A77-XXXX-MXXX-NXXX-XXXXXXXXXXXX").build());
      ExtractHighSpenders PT = new ExtractHighSpenders(1, 1L, bundles);

      PAssert.that(pp.apply(PT)).satisfies(out -> {
        Set<DeviceId> found = new HashSet<>();
        for (DeviceId id : out) {
          DeviceId canonicalId = __Utils.getCanonicalDeviceId(id);
          if (expected.contains(canonicalId)) {
            found.add(canonicalId);
            // ok
          } else {
            System.out.format("Unexpected id: %s\n", id.toString());
            for (DeviceId _id : expected) {
              System.out.format("%s\n", _id.toString());
            }
            fail();
          }
        }
        assertEquals(Iterables.size(out), found.size());
        assertEquals(expected.size(), found.size());
        return null;
      });
    }

    {
      ExtractHighSpenders PT = new ExtractHighSpenders(2, 1L, bundles);
      PAssert.that(pp.apply(PT)).empty();
    }

    {
      Set<DeviceId> expected = Sets.newHashSet(
          DeviceId.newBuilder().setOs(OsType.ANDROID).setUuid("5EEF43D1-XXXX-MXXX-NXXX-XXXXXXXXXXXX").build());
      ExtractHighSpenders PT = new ExtractHighSpenders(1, 2400L, bundles);

      PAssert.that(pp.apply(PT)).satisfies(out -> {
        Set<DeviceId> found = new HashSet<>();
        for (DeviceId id : out) {
          DeviceId canonicalId = __Utils.getCanonicalDeviceId(id);
          if (expected.contains(canonicalId)) {
            found.add(canonicalId);
            // ok
          } else {
            System.out.format("Unexpected id: %s\n", id.toString());
            for (DeviceId _id : expected) {
              System.out.format("%s\n", _id.toString());
            }
            fail();
          }
        }
        assertEquals(Iterables.size(out), found.size());
        assertEquals(expected.size(), found.size());
        return null;
      });
    }
    tp.run();
  }

  @Test
  public void testAddicts() {
    PCollection<PurchaserProfile> pp =
        tp.apply(Create.of(Arrays.asList(SAMPLES))).apply(PTransformGetPP).apply(PTransformMerge);

    {
      Set<DeviceId> expected = Sets.newHashSet(
          DeviceId.newBuilder().setOs(OsType.ANDROID).setUuid("54E94A77-XXXX-MXXX-NXXX-XXXXXXXXXXXX").build());

      PAssert.that(pp.apply(PTransformAddicts1)).satisfies(out -> {
        Set<DeviceId> found = new HashSet<>();
        for (DeviceId id : out) {
          DeviceId canonicalId = __Utils.getCanonicalDeviceId(id);
          if (expected.contains(canonicalId)) {
            found.add(canonicalId);
            // ok
          } else {
            System.out.format("Unexpected id: %s\n", id.toString());
            for (DeviceId _id : expected) {
              System.out.format("%s\n", _id.toString());
            }
            fail();
          }
        }
        assertEquals(Iterables.size(out), found.size());
        assertEquals(expected.size(), found.size());
        return null;
      });
    }

    {
      PAssert.that(pp.apply(PTransformAddicts2)).empty();
    }

    tp.run();
  }
}
