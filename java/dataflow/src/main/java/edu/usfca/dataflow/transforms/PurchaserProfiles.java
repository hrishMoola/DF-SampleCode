package edu.usfca.dataflow.transforms;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import edu.usfca.protobuf.Common;
import edu.usfca.protobuf.Event;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import edu.usfca.dataflow.utils.LogParser;
import edu.usfca.protobuf.Profile.PurchaserProfile;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;


import static edu.usfca.dataflow.transforms.CustomUtils.*;

/**
 * Task B.
 */
public class PurchaserProfiles {

  /**
   * Input PCollection contains log messages to be parsed using {@link LogParser#getIdAndPurchaseEvent(String)}.
   *
   * (For those who didn't complete Lab 05 -- these "logs" are synthetic but their format is legit. Load balancers (that
   * route incoming traffic to your server cluster) do leave logs that look exactly like these. Indeed, I adopted
   * Google's GCP Load Balancer logs for this project.)
   *
   * These log messages are assumed to be for an n-day period (where the exact value of n is not important to us yet).
   * Just think of it as a weekly or monthly dataset.
   *
   * Output PCollection must contain "PurchaseProfile" protos, one for each PurchaseEvent.
   *
   * Aggregation of these profiles happens in a different PTransform (mainly for unit test purposes)
   * {@link MergeProfiles}.
   *
   * If {@link LogParser#getIdAndPurchaseEvent(String)} returns a null value (because the log message is
   * invalid/corrupted), then simply ignore the returned value (don't include it in your output PC).
   *
   * As such, the output PCollection will always have no more elements than the input PCollection.
   *
   * You must first define your "PurchaseProfile" proto message (see profile.proto), then build your gradle project (to
   * generate proto template Java files based on your proto file), and complete implementing this PTransform.
   *
   * NOTE: Your proto message should NOT look similar to anyone else's (if two proto messages look similar, then it's
   * likely that someone copied it), because this project requires you figure out how to design a pipline and how to
   * represent the intermediate data. As such, DO NOT share/show your proto message definitions -- it'll be pretty
   * obvious.
   *
   * NOTE2: This is somewhat similar to obtaining DeviceProfiles from BidLogs (from previous projects).
   */
  public static class GetProfilesFromEvents extends PTransform<PCollection<String>, PCollection<PurchaserProfile>> {

    @Override
    public PCollection<PurchaserProfile> expand(PCollection<String> input) {

      return input.apply(ParDo.of(new DoFn<String, KV<Common.DeviceId, Event.PurchaseEvent>>(){
        @ProcessElement
        public void process(ProcessContext processContext){
          KV<Common.DeviceId, Event.PurchaseEvent> kv = LogParser.getIdAndPurchaseEvent(processContext.element());
          if(kv!=null)
            processContext.output(kv);
        }
      })).apply(ParDo.of(new DoFn<KV<Common.DeviceId, Event.PurchaseEvent>, PurchaserProfile>() {
        @ProcessElement
        public void process(ProcessContext processContext) {
          Event.PurchaseEvent purchaseEvent = processContext.element().getValue();
           PurchaserProfile.Builder purchaserProfileBuilder = PurchaserProfile.newBuilder();

           PurchaserProfile.PurchaserDetails.Builder purchaserDetailsBuilder = PurchaserProfile.PurchaserDetails.newBuilder();
           purchaserDetailsBuilder.setAmount(purchaseEvent.getAmount())
                   .setEventId(purchaseEvent.getEventId())
                   .setEventAt(purchaseEvent.getEventAt())
                   .setStoreName(purchaseEvent.getStore().name());
          purchaserProfileBuilder.setId(processContext.element().getKey())
                  .setPurchaseTotal(1).putBundlewiseDetails(purchaseEvent.getAppBundle(), edu.usfca.protobuf.Profile.PurchaserProfile.PurchaserList.newBuilder().addBundlewiseDetails(purchaserDetailsBuilder.build()).build());
        processContext.output(purchaserProfileBuilder.build());
        }
      }));
    }
  }

  /**
   * This is expected to be called on the output of {@link GetProfilesFromEvents} (unit tests do that, for instance).
   *
   * Given (yet-to-be-merged) PC of PurchaserProfile protos, this PTransform must combine profiles (by DeviceId).
   * 
   * It's up to you whether you choose to use GroupByKey or Combine.perKey (although Combine.perKey is recommended for
   * performance reasons).
   *
   * The logic for this PTransform will be determined based on how you defined your PurchaserProfile proto message.
   *
   * The only requirements (as far as unit tests go) are:
   *
   * (1) The output PCollection must have distinct DeviceIds (which must be valid)
   *
   * (2) "purchase_total" field's value must be correct for every PurchaserProfile (you can just sum them up; it's for
   * sanity check purposes).
   *
   * Other than these two, the grading system will NOT actually care about your PurchaserProfile protos (because it
   * doesn't know/care what you are doing with PurchaserProfile messages).
   */
  public static class MergeProfiles extends PTransform<PCollection<PurchaserProfile>, PCollection<PurchaserProfile>> {

    @Override
    public PCollection<PurchaserProfile> expand(PCollection<PurchaserProfile> input) {
      return input.apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), new TypeDescriptor<PurchaserProfile>(){}))
                .via((ProcessFunction<PurchaserProfile, KV<String, PurchaserProfile>>) item -> KV.of(JsonFormat.printer().print(item.getId()), item)))
                .apply(Combine.perKey(new CustomUtils.PurchaseProfilesCombiner()))
                      .apply(ParDo.of(new DoFn<KV<String, Aggre>, PurchaserProfile>() {
                  @ProcessElement
                  public void process(ProcessContext processContext) throws InvalidProtocolBufferException {
                    if(getDeviceId(processContext.element().getKey()).getUuid().contains("00000000"))
                      System.out.println("w");
                    processContext.output(PurchaserProfile.newBuilder()
                            .setId(getDeviceId(processContext.element().getKey()))
                            .putAllBundlewiseDetails(processContext.element().getValue().getBundleWiseList())
                            .setPurchaseTotal(processContext.element().getValue().getPurchaseTotal())
                            .build());
                  }
                }));
    }
    private Common.DeviceId getDeviceId(String key) throws InvalidProtocolBufferException {
      Common.DeviceId.Builder messageBuilder = Common.DeviceId.newBuilder();
      JsonFormat.parser().usingTypeRegistry(JsonFormat.TypeRegistry.getEmptyTypeRegistry()).merge(key, messageBuilder);
      return messageBuilder.build();
    }
  }
  public static class someDoFn extends DoFn<PurchaserProfile, Void> {
    @Setup
    public void setup() {
//      System.out.println("--- Starting to print out the contents. This is likely to be printed more than once. Why?");
    }

    @ProcessElement
    public void process(@Element PurchaserProfile elem) {
      if(elem.getId().getUuid().contains("CBA25CC2"))
        System.out.format("Profiles: %s\n", elem.toString());
    }
  }


//
//  Map<String, edu.usfca.protobuf.Profile.PurchaserProfile.PurchaserList> bundleWiseList = new HashMap<>();
//            for(int i = 0; i < accumulator.counter ; i ++){
//    edu.usfca.protobuf.Profile.PurchaserProfile.PurchaserList.Builder currentList = edu.usfca.protobuf.Profile.PurchaserProfile.PurchaserList.newBuilder(bundleWiseList.getOrDefault(accumulator.bundle[i], edu.usfca.protobuf.Profile.PurchaserProfile.PurchaserList.getDefaultInstance()));
//    currentList.addBundlewiseDetails(edu.usfca.protobuf.Profile.PurchaserProfile.PurchaserDetails.newBuilder().setAmount(accumulator.amount[i]).setEventAt(accumulator.eventAt[i]).setEventId("123L").setStoreName("some").build());
//  }
//  edu.usfca.protobuf.Profile.PurchaserProfile.Builder purchaseProfileBuilder = edu.usfca.protobuf.Profile.PurchaserProfile.newBuilder();
//            return purchaseProfileBuilder.setPurchaseTotal(accumulator.counter)
//          .putAllBundlewiseDetails(bundleWiseList).build();
//}

}
