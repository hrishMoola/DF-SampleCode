package edu.usfca.dataflow.transforms;

import edu.usfca.protobuf.Profile;
import org.apache.beam.sdk.transforms.Combine;

import java.io.Serializable;
import java.util.*;


public class CustomUtils {
    public static class PurchaseProfilesCombiner extends Combine.CombineFn<Profile.PurchaserProfile, Aggre, Aggre> {

        @Override
        public Aggre createAccumulator() {
            return new Aggre();
        }

        @Override
        public Aggre addInput(Aggre mutableAccumulator, Profile.PurchaserProfile input) {
            input.getBundlewiseDetailsMap().forEach((bundle, details) -> {
                mutableAccumulator.add(bundle, edu.usfca.protobuf.Profile.PurchaserProfile.PurchaserDetails.newBuilder().setAmount(details.getBundlewiseDetailsList().get(0)
                        .getAmount()).setEventAt(details.getBundlewiseDetailsList().get(0).getEventAt())
                        .setEventId("123L").setStoreName("something").build());              //junk values for fields not in use for this project
            });
            return mutableAccumulator;
        }

        @Override
        public Aggre mergeAccumulators(Iterable<Aggre> accumulators) {
            Aggre mergedAccum = new Aggre();
            for (Aggre aggre : accumulators) {
                mergedAccum.add(aggre);
            }
            return mergedAccum;
        }

        @Override
        public Aggre extractOutput(Aggre accumulator) {
            return accumulator;
        }

    }





    static class Aggre implements Serializable{
        private final Map<String, Profile.PurchaserProfile.PurchaserList> bundleWiseList = new HashMap<>();
        private Integer purchaseTotal = 0;

        public void add(String bundle, Profile.PurchaserProfile.PurchaserDetails purchaserDetails){
            edu.usfca.protobuf.Profile.PurchaserProfile.PurchaserList.Builder currentList = edu.usfca.protobuf.Profile.PurchaserProfile.PurchaserList.newBuilder(bundleWiseList.getOrDefault(bundle, edu.usfca.protobuf.Profile.PurchaserProfile.PurchaserList.getDefaultInstance()));
            currentList.addBundlewiseDetails(purchaserDetails);
            bundleWiseList.put(bundle,currentList.build());
            purchaseTotal++;
        }

        public Map<String, Profile.PurchaserProfile.PurchaserList> getBundleWiseList(){
            return bundleWiseList;
        }

        public void add(Aggre aggre) {
            aggre.getBundleWiseList().forEach((bundle, bundleWiseList) -> {
                bundleWiseList.getBundlewiseDetailsList().forEach(purchaserDetails -> {
                    this.add(bundle,purchaserDetails);
                });
            });
        }

        public Integer getPurchaseTotal() {
            return purchaseTotal;
        }
    }

}







