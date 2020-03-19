package edu.usfca.dataflow.transforms;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.Iterables;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
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
      return input.apply(new GetMergedInAppPurchaseProfiles("bundle")).apply(Values.create());
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

      return input.apply(ParDo.of(new DoFn<PurchaserProfile, DeviceId>() {
        @ProcessElement
        public void process(ProcessContext processContext) throws InvalidProtocolBufferException {
          Map<String, PurchaserProfile.PurchaserList> bundleWiseDetails = processContext.element().getBundlewiseDetailsMap();
          AtomicReference<Long> summedAmount = new AtomicReference<>(0L);
          AtomicReference<Integer> totalPurchases = new AtomicReference<>(0);
          bundleWiseDetails.forEach((bundle, detailsList) -> {
            if (bundles.contains(bundle)) {
              detailsList.getBundlewiseDetailsList().forEach(purchaserDetails -> {
                summedAmount.updateAndGet(v -> v + purchaserDetails.getAmount());
              });
              totalPurchases.updateAndGet(v -> v + detailsList.getBundlewiseDetailsCount());
            }
            if (summedAmount.get() >= totalAmount && totalPurchases.get() >= numPurchases)
              processContext.output(processContext.element().getId());
          });
        }
      })).apply(Distinct.create());
    }
  }

    /***
     * Helper PTransform Method. Gets a KV of String to InAppPurchase Profile.
     * Key is of type String, and can be Bundle name or Device Id, as per requirement.
     * The subsequent grouping and merging then comes out with respect to the key in question. This is done as merging logic can be same, but grouping parameter is variable.
     */

    public static class GetMergedInAppPurchaseProfiles extends PTransform<PCollection<PurchaserProfile>, PCollection<KV<String, InAppPurchaseProfile>>> {

      final String key;

      GetMergedInAppPurchaseProfiles(String keyType) {
        this.key = keyType;
      }

      @Override
      public PCollection<KV<String, InAppPurchaseProfile>> expand(PCollection<PurchaserProfile> input) {

        return input.apply(new CreateMultipleProfiles(this.key))
                .apply(GroupByKey.create())
                .apply(ParDo.of(new DoFn<KV<String, Iterable<InAppPurchaseProfile>>, KV<String, InAppPurchaseProfile>>() {
                  @ProcessElement
                  public void process(ProcessContext processContext) {
                    InAppPurchaseProfile.Builder mergedInApp = InAppPurchaseProfile.newBuilder();
                    processContext.element().getValue().forEach(inAppPurchaseProfile -> {
                      mergedInApp.setBundle(inAppPurchaseProfile.getBundle())
                              .setTotalAmount(mergedInApp.getTotalAmount() + inAppPurchaseProfile.getTotalAmount());
                    });
                    mergedInApp.setNumPurchasers(Iterables.size(processContext.element().getValue()));
                    processContext.output(KV.of(processContext.element().getKey(), mergedInApp.build()));
                  }
                }));
      }
    }

    /**
     * Helper
     */

    public static class CreateMultipleProfiles extends PTransform<PCollection<PurchaserProfile>, PCollection<KV<String, InAppPurchaseProfile>>> {

      final String key;

      CreateMultipleProfiles(String keyType) {
        this.key = keyType;
      }

      @Override
      public PCollection<KV<String, InAppPurchaseProfile>> expand(PCollection<PurchaserProfile> input) {
        PCollectionView<String> key = input.getPipeline().apply(Create.of(this.key)).apply(View.asSingleton());

        return input.apply(ParDo.of(new DoFn<PurchaserProfile, KV<String, InAppPurchaseProfile>>() {
          @ProcessElement
          public void process(ProcessContext processContext) throws InvalidProtocolBufferException {
            Map<String, PurchaserProfile.PurchaserList> bundleWiseDetails = processContext.element().getBundlewiseDetailsMap();
            bundleWiseDetails.forEach((bundle, detailsList) -> {
              String keyType = processContext.sideInput(key);
              InAppPurchaseProfile.Builder inappBuilder = InAppPurchaseProfile.newBuilder();
              AtomicReference<Long> totalAmount = new AtomicReference<>(0L);
              detailsList.getBundlewiseDetailsList().forEach(purchaserDetails -> {
                totalAmount.updateAndGet(v -> v + purchaserDetails.getAmount());
              });
              inappBuilder.setBundle(bundle).setNumPurchasers(1).setTotalAmount(totalAmount.get());
              if (keyType.equalsIgnoreCase("bundle"))
                processContext.output(KV.of(bundle, inappBuilder.build()));
            });
          }
        }).withSideInputs(key));
      }
    }


    /**
     * This will be applied to the output of {@link MergeProfiles}.
     * <p>
     * NOTE: This is probably the hardest subtask of the three. I recommend you finish other tasks first, and work on this
     * part separately at last.
     * <p>
     * This PTransform should return a PC of DeviceIds (users) who are considered as "addicts."
     * <p>
     * It takes two parameters: String parameter "bundle" that specifies the bundle of our interests and integer parameter
     * "x" representing the number of consecutive (calendar) days.
     * <p>
     * Return a PC of DeviceIds of the users who made at least one purchase per day for {@code x} consecutive (calendar)
     * days (in UTC timezone) for the given app ("bundle").
     * <p>
     * For instance, if a user made a purchase on "2020-02-01T10:59:59.999Z", another on "2020-02-02T20:59:59.999Z", and
     * one more on "2020-02-03T23:59:59.999Z" (for the same app), then this user would be considered as an addict when
     * {@code x=3} (we only consider the "dates", and since this user made at least one purchase on each of Feb 01, Feb
     * 02, and Feb 03). On the other hand: if a user made a purchase on "2020-02-01T10:59:59.999Z", another on
     * "2020-02-02T20:59:59.999Z", and * one more on "2020-02-02T23:59:59.999Z" (for the same app), then this user would
     * NOT be considered as an addict. (this user made one purchase on Feb 01 and two on Feb 02, so that is NOT 3
     * consecutive days).
     * <p>
     * You may find https://currentmillis.com/ useful (for checking unix millis and converting it to UTC date).
     * <p>
     * NOTE: If "bundle" is null/blank, throw IllegalArgumentException (use the usual apache.lang3 utility). If "x" is 0
     * or less, then throw IllegalArgumentException as well.
     * <p>
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

        Map<String, String> inputs = new HashMap<>();
        inputs.put("bundle", bundle);
        inputs.put("days", String.valueOf(CONSEC_DAYS));
        PCollectionView<Map<String, String>> inputListView = input.getPipeline().apply(Create.of(inputs)).apply(View.asMap());
        return input.apply(ParDo.of(new DoFn<PurchaserProfile, DeviceId>() {
          @ProcessElement
          public void process(ProcessContext processContext) throws InvalidProtocolBufferException {
            Map<String, PurchaserProfile.PurchaserList> bundleWiseDetails = processContext.element().getBundlewiseDetailsMap();
            String inputBundle = processContext.sideInput(inputListView).get("bundle");
            int inputDays = Integer.parseInt(processContext.sideInput(inputListView).get("days"));
            bundleWiseDetails.forEach((bundle, detailsList) -> {
              if (bundle.equalsIgnoreCase(inputBundle)) {
                Set<Integer> events = new TreeSet<>();
                detailsList.getBundlewiseDetailsList().forEach(purchaserDetails -> {
                  events.add(millisToDay(purchaserDetails.getEventAt()));
                });
                if (inputDays == 1)
                  processContext.output(processContext.element().getId());
                else{
                  int consecCount = 1 ;
                  if (events.size() >= inputDays) {
                    Integer[] eventsArray = events.toArray(new Integer[0]);
                    for (int i = 0; i < eventsArray.length - 1; i++) {
                      if (eventsArray[i + 1] - eventsArray[i] != 1) {
                          consecCount = 1;
                      } else if(++consecCount == inputDays) {
                          processContext.output(processContext.element().getId());
                          break;
                        }
                    }
                }
              }
              }
            });
          }
        }).withSideInputs(inputListView));
      }
    }
  }

