package edu.usfca.dataflow.transforms;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;

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

import edu.usfca.dataflow.transforms.ExtractData.ExtractInAppPurchaseData;
import edu.usfca.dataflow.transforms.PurchaserProfiles.GetProfilesFromEvents;
import edu.usfca.dataflow.transforms.PurchaserProfiles.MergeProfiles;
import edu.usfca.protobuf.Common.OsType;
import edu.usfca.protobuf.Profile.InAppPurchaseProfile;
import edu.usfca.protobuf.Profile.PurchaserProfile;

public class __TestPurchaserProfiles {
  // Grading system will use this timeout to make sure it does not run indefinitely.
  // The timeout provided below should be more than sufficient (yet, if it turns out it's too short, I'll adjust it and
  // re-grade so you don't have to worry about that).
  // You can disable this for your local tests, though (just remove the following two lines).
  @Rule
  public Timeout timeout = Timeout.millis(10000);

  @Rule
  public final transient TestPipeline tp = TestPipeline.create();

  @Before
  public void before() {
    tp.getOptions().setStableUniqueNames(CheckEnabled.OFF);
  }

  // Try these on json formatter website(s), like https://jsonformatter.curiousconcept.com/
  // These are taken from sample-tiny.txt in the judge/resources directory.
  // same files can also be found under dataflow/resources directory, for your convenience.
  // Disclaimer: remoteIp and other fields are randomly generated, and uuids also anonymized.
  static final String[] SAMPLES = new String[] {
      "{\"httpRequest\":{\"remoteIp\":\"135.131.199.249\",\"requestMethod\":\"GET\",\"status\":200,\"serverIp\":\"127.0.0.1\",\"requestUrl\":\"https://www.cs.usfca.edu/you/are/awesome?minecraft=pretty+fun&amount=390.244&event_type=PURChAsE&event_id=bQVfQKkjeDJRWH92TF&decoded=wow%21&store=AppsTore&gps_adid=06798d72-xxxx-Mxxx-Nxxx-xxxxxxxxxxxx&bundle=iIHl65T0o\"},\"insertId\":\"k2I013rv\",\"logName\":\"projects/beer-spear/logs/requests\",\"timestamp\":\"2020-02-20T01:55:35.358731Z\",\"spanId\":\"Pyg3oCrhLgh9k4B\",\"trace\":\"projects/beer-spear/traces/oU0Qm8k4rJpg9qyh\"}",
      "{\"httpRequest\":{\"remoteIp\":\"54.148.98.94\",\"requestMethod\":\"GET\",\"status\":200,\"serverIp\":\"127.0.0.1\",\"requestUrl\":\"https://www.cs.usfca.edu/you/are/awesome?amount=1105.691&event_type=PURcHASE&event_id=XfCS1xEKfJpgVss0Kxe&decoded=wow%21&store=aPpstore&gps_adid=407cc9c9-xxxx-Mxxx-Nxxx-xxxxxxxxxxxx&bundle=DChu2S719\"},\"insertId\":\"lSpRhcyyvb\",\"logName\":\"projects/beer-spear/logs/requests\",\"timestamp\":\"2020-02-20T01:55:48.194991Z\",\"spanId\":\"F2ChTShB\",\"trace\":\"projects/beer-spear/traces/mR6KF2Y0o2r96lwmjyhL\"}",
      "{\"httpRequest\":{\"remoteIp\":\"81.153.19.87\",\"requestMethod\":\"GET\",\"status\":200,\"serverIp\":\"127.0.0.1\",\"requestUrl\":\"https://www.cs.usfca.edu/you/are/awesome?ios_idfa=F199DFDE-xxxx-Mxxx-Nxxx-xxxxxxxxxxxx&amount=1869.919&event_type=PURChaSE&event_id=Xw3rY5&store=AppsTorE&bundle=F7GEqu1ZIlMthlv25rjvimSc\"},\"insertId\":\"WHTy7S3p8\",\"logName\":\"projects/beer-spear/logs/requests\",\"timestamp\":\"2020-02-20T01:53:59.715888Z\",\"spanId\":\"wiYcjQj5BqFG\",\"trace\":\"projects/beer-spear/traces/IkUh223zqzTFjms\"}"};

  // Almost identical to SAMPLES, with minor changes.
  static final String[] SAMPLES2 = new String[] {
      "{\"httpRequest\":{\"remoteIp\":\"135.131.199.249\",\"requestMethod\":\"GET\",\"status\":200,\"serverIp\":\"127.0.0.1\",\"requestUrl\":\"https://www.cs.usfca.edu/you/are/awesome?minecraft=pretty+fun&amount=1390.444&event_type=PURChAsE&event_id=bQVfQKkjeDJRWH92TF&decoded=wow%21&store=AppsTore&gps_adid=06798d72-xxxx-Mxxx-Nxxx-xxxxxxxxxxxx&bundle=iIHl65T0o\"},\"insertId\":\"k2I013rv\",\"logName\":\"projects/beer-spear/logs/requests\",\"timestamp\":\"2020-02-20T01:55:35.358731Z\",\"spanId\":\"Pyg3oCrhLgh9k4B\",\"trace\":\"projects/beer-spear/traces/oU0Qm8k4rJpg9qyh\"}",
      "{\"httpRequest\":{\"remoteIp\":\"54.148.98.94\",\"requestMethod\":\"GET\",\"status\":200,\"serverIp\":\"127.0.0.1\",\"requestUrl\":\"https://www.cs.usfca.edu/you/are/awesome?amount=2105.991&event_type=PURcHASE&event_id=XfCS1xEKfJpgVss0Kxe&decoded=wow%21&store=aPpstore&gps_adid=407cc9c9-xxxx-Mxxx-Nxxx-xxxxxxxxxxxx&bundle=DChu2S719\"},\"insertId\":\"lSpRhcyyvb\",\"logName\":\"projects/beer-spear/logs/requests\",\"timestamp\":\"2020-02-20T01:55:48.194991Z\",\"spanId\":\"F2ChTShB\",\"trace\":\"projects/beer-spear/traces/mR6KF2Y0o2r96lwmjyhL\"}",
      "{\"httpRequest\":{\"remoteIp\":\"81.153.19.87\",\"requestMethod\":\"GET\",\"status\":200,\"serverIp\":\"127.0.0.1\",\"requestUrl\":\"https://www.cs.usfca.edu/you/are/awesome?ios_idfa=F199DFDE-xxxx-Mxxx-Nxxx-xxxxxxxxxxxx&amount=2869.019&event_type=PURChaSE&event_id=Xw3rY5&store=AppsTorE&bundle=F7GEqu1ZIlMthlv25rjvimSc\"},\"insertId\":\"WHTy7S3p8\",\"logName\":\"projects/beer-spear/logs/requests\",\"timestamp\":\"2020-02-20T01:53:59.715888Z\",\"spanId\":\"wiYcjQj5BqFG\",\"trace\":\"projects/beer-spear/traces/IkUh223zqzTFjms\"}"};

  @Test
  public void testSamples01() {
    PCollection<String> inputLogs = tp.apply(Create.of(Arrays.asList(SAMPLES[0], SAMPLES[2], SAMPLES[1])));

    PCollection<PurchaserProfile> purchasers = inputLogs.apply(new GetProfilesFromEvents());

    PAssert.that(purchasers).satisfies(out -> {
      assertEquals(3, Iterables.size(out));
      int found = 0;
      for (PurchaserProfile pp : out) {

        assertEquals(1, pp.getPurchaseTotal());

        if (pp.getId().getUuid().substring(0, 8).equalsIgnoreCase("f199dfde")) {
          found ^= 1 << 0;
          assertEquals(OsType.IOS, pp.getId().getOs());
        } else if (pp.getId().getUuid().substring(0, 8).equalsIgnoreCase("407cc9c9")) {
          found ^= 1 << 1;
          assertEquals(OsType.ANDROID, pp.getId().getOs());
        } else if (pp.getId().getUuid().substring(0, 8).equalsIgnoreCase("06798d72")) {
          found ^= 1 << 2;
          assertEquals(OsType.ANDROID, pp.getId().getOs());
        } else {
          fail();
        }
      }
      assertEquals((1 << 3) - 1, found);
      return null;
    });

    tp.run();
  }

  @Test
  public void testSamples02() {
    PCollection<String> inputLogs = tp.apply(Create.of(Arrays.asList(SAMPLES[0], SAMPLES[0])));

    PCollection<PurchaserProfile> purchasers = inputLogs.apply(new GetProfilesFromEvents());

    PAssert.that(purchasers).satisfies(out -> {
      assertEquals(2, Iterables.size(out));
      int found = 0;
      for (PurchaserProfile pp : out) {

        assertEquals(1, pp.getPurchaseTotal());

        if (pp.getId().getUuid().substring(0, 8).equalsIgnoreCase("06798d72")) {
          found++;
          assertEquals(OsType.ANDROID, pp.getId().getOs());
        } else {
          fail();
        }
      }
      assertEquals(2, found);
      return null;
    });

    tp.run();
  }

  @Test
  public void testDaily01() {
    PCollection<String> inputLogs = tp.apply(Create.of(Arrays.asList(SAMPLES[0], SAMPLES[2], SAMPLES[1])));

    PCollection<PurchaserProfile> merged = inputLogs.apply(new GetProfilesFromEvents()).apply(new MergeProfiles());

    PAssert.that(merged).satisfies(out -> {
      assertEquals(3, Iterables.size(out));
      int found = 0;
      for (PurchaserProfile pp : out) {

        assertEquals(1, pp.getPurchaseTotal());

        if (pp.getId().getUuid().substring(0, 8).equalsIgnoreCase("f199dfde")) {
          found ^= 1 << 0;
          assertEquals(OsType.IOS, pp.getId().getOs());
        } else if (pp.getId().getUuid().substring(0, 8).equalsIgnoreCase("407cc9c9")) {
          found ^= 1 << 1;
          assertEquals(OsType.ANDROID, pp.getId().getOs());
        } else if (pp.getId().getUuid().substring(0, 8).equalsIgnoreCase("06798d72")) {
          found ^= 1 << 2;
          assertEquals(OsType.ANDROID, pp.getId().getOs());
        } else {
          fail();
        }
      }
      assertEquals((1 << 3) - 1, found);
      return null;
    });

    tp.run();
  }

  @Test
  public void testDaily02() {
    PCollection<String> inputLogs =
        tp.apply(Create.of(Arrays.asList(SAMPLES[0], SAMPLES[2], SAMPLES[1], SAMPLES2[0], SAMPLES2[1], SAMPLES2[2])));

    PCollection<PurchaserProfile> merged = inputLogs.apply(new GetProfilesFromEvents()).apply(new MergeProfiles());

    PAssert.that(merged).satisfies(out -> {
      assertEquals(3, Iterables.size(out));
      int found = 0;
      for (PurchaserProfile pp : out) {

        assertEquals(2, pp.getPurchaseTotal());

        if (pp.getId().getUuid().substring(0, 8).equalsIgnoreCase("f199dfde")) {
          found ^= 1 << 0;
          assertEquals(OsType.IOS, pp.getId().getOs());
        } else if (pp.getId().getUuid().substring(0, 8).equalsIgnoreCase("407cc9c9")) {
          found ^= 1 << 1;
          assertEquals(OsType.ANDROID, pp.getId().getOs());
        } else if (pp.getId().getUuid().substring(0, 8).equalsIgnoreCase("06798d72")) {
          found ^= 1 << 2;
          assertEquals(OsType.ANDROID, pp.getId().getOs());
        } else {
          fail();
        }
      }
      assertEquals((1 << 3) - 1, found);
      return null;
    });

    tp.run();
  }

  @Test
  public void testExtract01() {
    PCollection<String> inputLogs = tp.apply(Create.of(Arrays.asList(SAMPLES[0], SAMPLES[2], SAMPLES[1])));

    PCollection<InAppPurchaseProfile> merged =
        inputLogs.apply(new GetProfilesFromEvents()).apply(new MergeProfiles()).apply(new ExtractInAppPurchaseData());

    PAssert.that(merged).satisfies(out -> {
      assertEquals(3, Iterables.size(out));
      int found = 0;
      for (InAppPurchaseProfile iapp : out) {
        assertEquals(1, iapp.getNumPurchasers());

        if (iapp.getBundle().equalsIgnoreCase("dchu2s719")) {
          found ^= 1 << 0;
          assertEquals(1106, iapp.getTotalAmount());
        } else if (iapp.getBundle().equalsIgnoreCase("iihl65t0o")) {
          found ^= 1 << 1;
          assertEquals(390, iapp.getTotalAmount());
        } else if (iapp.getBundle().equalsIgnoreCase("f7gequ1zilmthlv25rjvimsc")) {
          found ^= 1 << 2;
          assertEquals(1870, iapp.getTotalAmount());
        } else {
          fail();
        }
      }
      assertEquals((1 << 3) - 1, found);
      return null;
    });

    tp.run();
  }

  @Test
  public void testExtract02() {
    PCollection<String> inputLogs =
        tp.apply(Create.of(Arrays.asList(SAMPLES[0], SAMPLES[2], SAMPLES[1], SAMPLES2[0], SAMPLES2[1], SAMPLES2[2])));

    PCollection<InAppPurchaseProfile> merged =
        inputLogs.apply(new GetProfilesFromEvents()).apply(new MergeProfiles()).apply(new ExtractInAppPurchaseData());

    PAssert.that(merged).satisfies(out -> {
      assertEquals(3, Iterables.size(out));
      int found = 0;
      for (InAppPurchaseProfile iapp : out) {
        assertEquals(1, iapp.getNumPurchasers());

        if (iapp.getBundle().equalsIgnoreCase("dchu2s719")) {
          found ^= 1 << 0;
          assertEquals(2106 + 1106, iapp.getTotalAmount());
        } else if (iapp.getBundle().equalsIgnoreCase("iihl65t0o")) {
          found ^= 1 << 1;
          assertEquals(1390 + 390, iapp.getTotalAmount());
        } else if (iapp.getBundle().equalsIgnoreCase("f7gequ1zilmthlv25rjvimsc")) {
          found ^= 1 << 2;
          assertEquals(2869 + 1870, iapp.getTotalAmount());
        } else {
          fail();
        }
      }
      assertEquals((1 << 3) - 1, found);
      return null;
    });

    tp.run();
  }
}
