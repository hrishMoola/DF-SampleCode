package edu.usfca.dataflow.transforms;

import java.util.Set;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.StringUtils;

import edu.usfca.dataflow.transforms.PurchaserProfiles.MergeProfiles;
import edu.usfca.protobuf.Common.DeviceId;
import edu.usfca.protobuf.Profile.InAppPurchaseProfile;
import edu.usfca.protobuf.Profile.PurchaserProfile;

/**
 * Task C: You need to implement three PTransforms.
 *
 * Each of these PTransforms will be applied to the output of {@link PurchaserProfiles.MergeProfiles}.
 *
 * That is, something like: {@code pc.apply(new MergeProfiles()).apply(?)} where ? is one of the three PTransforms in
 * this class.
 *
 * The grading system will test the output PCollections of these three PTransforms (check the provided unit tests).
 */
public class ExtractData {
  /**
   * This will be applied to the output of {@link MergeProfiles}.
   *
   * InAppPurchaseProfile (IAPP) is defined in profile.proto, and you should NOT modify it.
   *
   * Based on the input PCollection of Purchaser Profiles (which have been aggregated, recall), this PTransform should
   * produce a PC of IAPPs such that
   *
   * (1) "bundle"s are distinct,
   *
   * (2) "num_purchaser" is the number of unique purchasers (for a given app) based on DeviceId, and
   *
   * (3) "total_amount" is the sum of purchase amounts (for a given app).
   *
   * Note that you do not have to check for errors (in input) or handle "corrupted data" in this PTransform.
   */
  public static class ExtractInAppPurchaseData
      extends PTransform<PCollection<PurchaserProfile>, PCollection<InAppPurchaseProfile>> {

    @Override
    public PCollection<InAppPurchaseProfile> expand(PCollection<PurchaserProfile> input) {
      return null;
    }
  }

  /**
   * This will be applied to the output of {@link MergeProfiles}.
   *
   * This PTransform should return a PC of DeviceIds that are "high spenders".
   *
   * It takes three parameters (to define high spenders):
   *
   * (1) an integer, "x",
   *
   * (2) an integer, "y", and
   *
   * (3) a set of "bundles".
   *
   * Return a PC of DeviceIds who
   *
   * (i) made {@code x} or more purchases AND
   *
   * (ii) spent {@code y} or more
   *
   * across the apps specified in the set {@code bundles}.
   *
   * Note that you should take {@code bundles} into account (because not all purchase events are interesting).
   *
   * Also note that your output PCollection can be legitimately empty.
   *
   * Note that you do not have to check for errors (in input) or handle "corrupted data" in this PTransform.
   */
  public static class ExtractHighSpenders extends PTransform<PCollection<PurchaserProfile>, PCollection<DeviceId>> {

    final int numPurchases;
    final long totalAmount;
    final Set<String> bundles;

    public ExtractHighSpenders(int numPurchases, long totalAmount, Set<String> bundles) {
      this.numPurchases = numPurchases;
      this.totalAmount = totalAmount;
      this.bundles = bundles;
    }

    @Override
    public PCollection<DeviceId> expand(PCollection<PurchaserProfile> input) {
      return null;
    }
  }

  /**
   * This will be applied to the output of {@link MergeProfiles}.
   *
   * NOTE: This is probably the hardest subtask of the three. I recommend you finish other tasks first, and work on this
   * part separately at last.
   *
   * This PTransform should return a PC of DeviceIds (users) who are considered as "addicts."
   *
   * It takes two parameters: String parameter "bundle" that specifies the bundle of our interests and integer parameter
   * "x" representing the number of consecutive (calendar) days.
   *
   * Return a PC of DeviceIds of the users who made at least one purchase per day for {@code x} consecutive (calendar)
   * days (in UTC timezone) for the given app ("bundle").
   *
   * For instance, if a user made a purchase on "2020-02-01T10:59:59.999Z", another on "2020-02-02T20:59:59.999Z", and
   * one more on "2020-02-03T23:59:59.999Z" (for the same app), then this user would be considered as an addict when
   * {@code x=3} (we only consider the "dates", and since this user made at least one purchase on each of Feb 01, Feb
   * 02, and Feb 03). On the other hand: if a user made a purchase on "2020-02-01T10:59:59.999Z", another on
   * "2020-02-02T20:59:59.999Z", and * one more on "2020-02-02T23:59:59.999Z" (for the same app), then this user would
   * NOT be considered as an addict. (this user made one purchase on Feb 01 and two on Feb 02, so that is NOT 3
   * consecutive days).
   *
   * You may find https://currentmillis.com/ useful (for checking unix millis and converting it to UTC date).
   *
   * NOTE: If "bundle" is null/blank, throw IllegalArgumentException (use the usual apache.lang3 utility). If "x" is 0
   * or less, then throw IllegalArgumentException as well.
   *
   * NOTE2: For your convenience, millisToDay method is provided (see __TestMillisToDay).
   */
  public static class ExtractAddicts extends PTransform<PCollection<PurchaserProfile>, PCollection<DeviceId>> {

    final String bundle;
    final int CONSEC_DAYS;

    public ExtractAddicts(String bundle, int x) {
      if (StringUtils.isBlank(bundle)) {
        throw new IllegalArgumentException("");
      }
      if (x <= 0) {
        throw new IllegalArgumentException("");
      }
      this.bundle = bundle;
      this.CONSEC_DAYS = x;
    }

    public static int millisToDay(long millisInUtc) {
      // You do NOT have to use this method, but it's provided as reference/example.
      // This "rounds millis down" to the calendar day, and you should use that to determine whether a person made
      // purchases for x consecutive days or not. You can find examples in unit tests.
      return (int) (millisInUtc / 1000L / 3600L / 24L);
    }

    @Override
    public PCollection<DeviceId> expand(PCollection<PurchaserProfile> input) {
      return null;
    }
  }
}
