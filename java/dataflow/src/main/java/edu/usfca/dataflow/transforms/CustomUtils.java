package edu.usfca.dataflow.transforms;

import edu.usfca.protobuf.Profile;
import org.apache.beam.sdk.transforms.Combine;

import java.util.HashMap;
import java.util.Map;


public class CustomUtils {
    public static class PurchaseProfilesCombiner extends Combine.CombineFn<Profile.PurchaserProfile, Profile.PurchaserProfile, Profile.PurchaserProfile> {
        @Override
        public Profile.PurchaserProfile createAccumulator() {
            return Profile.PurchaserProfile.getDefaultInstance();
        }

        @Override
        public Profile.PurchaserProfile addInput(Profile.PurchaserProfile mutableAccumulator, Profile.PurchaserProfile input) {
            mutableAccumulator = Profile.PurchaserProfile.newBuilder(input).build();
            return mutableAccumulator;
        }

        @Override
        public Profile.PurchaserProfile mergeAccumulators(Iterable<Profile.PurchaserProfile> accumulators) {
            Map<String, Profile.PurchaserProfile.PurchaserList> newMap = new HashMap<>();
            Profile.PurchaserProfile.Builder mergedProfile =  Profile.PurchaserProfile.newBuilder();
            int purchaseTotal = 0;
            for( Profile.PurchaserProfile profile : accumulators) {
                if(profile.getPurchaseTotal() > 0 ){
                    mergedProfile = Profile.PurchaserProfile.newBuilder(profile);
                    Map<String, Profile.PurchaserProfile.PurchaserList> bundleWiseMap = profile.getBundlewiseDetailsMap();
                    String bundle = (String) bundleWiseMap.keySet().toArray()[0];
                    purchaseTotal += profile.getPurchaseTotal();

                    if(newMap.containsKey(bundle)){
                        Profile.PurchaserProfile.PurchaserList.Builder updatePurchaseList = Profile.PurchaserProfile.PurchaserList.newBuilder(newMap.get(bundle));
                        updatePurchaseList.addBundlewiseDetails(bundleWiseMap.get(bundle).getBundlewiseDetails(0));
                        newMap.put(bundle,updatePurchaseList.build());
                    } else {
                        newMap.putAll(bundleWiseMap);
                    }
                }
            };
            mergedProfile.clearBundlewiseDetails();
            mergedProfile.putAllBundlewiseDetails(newMap);
            mergedProfile.setPurchaseTotal(purchaseTotal);
            return mergedProfile.build();
        }

        @Override
        public Profile.PurchaserProfile extractOutput(Profile.PurchaserProfile accumulator) {
            return accumulator;
        }
    }
}
