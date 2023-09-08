package org.cloudbus.cloudsim.serverless;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerPe;
import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.schedulers.ContainerSchedulerTimeSharedOverSubscription;
import org.cloudbus.cloudsim.core.CloudSim;

import java.util.ArrayList;
import java.util.List;

public class ServerlessContainerScheduler extends ContainerSchedulerTimeSharedOverSubscription {
    /**
     * Instantiates a new container scheduler time shared.
     *
     * @param pelist the pelist
     */
    public ServerlessContainerScheduler(List<? extends ContainerPe> pelist) {
        super(pelist);
    }

    public boolean reAllocatePesForContainer(Container container, double newMIPS) {

        boolean result = reAllocatePesForContainer(container.getUid(), newMIPS, container);
        updatePeProvisioning();
        return result;
    }

    public boolean isSuitableForContainer(Container container, ServerlessInvoker vm) {
        int assignedPes = 0;
        for (ContainerPe pe : getPeList()) {
            System.out.println(CloudSim.clock() + " >>>>>>>>>>>>>>Available pe mips in vm # "+vm.getId() +" is " +pe.getContainerPeProvisioner().getAvailableMips() + " Needed mips for container # "+container.getId() + " is " + container.getMips());

            double tmp = (pe.getContainerPeProvisioner().getAvailableMips());
            if (tmp > container.getMips()) {
                assignedPes++ ;
                if(assignedPes == container.getNumberOfPes()){
                    break;
                }

            }
        }

        return assignedPes == container.getNumberOfPes();
    }

    public boolean reAllocatePesForContainer(String containerUid, double newMips, Container container) {
        double totalRequestedMips = 0;
        double oldMips = container.getMips()* container.getNumberOfPes();

        // if the requested mips is bigger than the capacity of a single PE, we cap
        // the request to the PE's capacity
        List<Double> mipsShareRequested = new ArrayList<Double>();
        for (int x=0; x < container.getNumberOfPes(); x++){
            mipsShareRequested.add(newMips);
            totalRequestedMips += newMips;
        }
//        mipsShareRequested.add(newMips);
//        List<Double> mipsShareRequestedCapped = new ArrayList<Double>();
//        double peMips = getPeCapacity();
//
//        if (newMips > peMips) {
//            mipsShareRequestedCapped.add(peMips);
//            totalRequestedMips += peMips;
//        } else {
//            mipsShareRequestedCapped.add(newMips);
//            totalRequestedMips += newMips;
//        }




        if (getContainersMigratingIn().contains(containerUid)) {
            // the destination host only experience 10% of the migrating VM's MIPS
            totalRequestedMips = 0.0;
        }else {

            getMipsMapRequested().put(containerUid, mipsShareRequested);
//            setPesInUse(getPesInUse() + mipsShareRequested.size());

        }

        //System.out.println("Debugging: Containerscheduler: All available MIPS before reallocation "+getAvailableMips()+" Extra MIPS requested"+ (totalRequestedMips-oldMips));
        if (getAvailableMips() >= (totalRequestedMips-oldMips)) {
            List<Double> mipsShareAllocated = new ArrayList<Double>();
            for (Double mipsRequested : mipsShareRequested) {
//                if (getContainersMigratingOut().contains(containerUid)) {
//                    // performance degradation due to migration = 10% MIPS
//                    mipsRequested *= 0.9;
//                } else
                if (!getContainersMigratingIn().contains(containerUid)) {
                    // the destination host only experience 10% of the migrating VM's MIPS
                    mipsShareAllocated.add(mipsRequested);

                    /** Add the current MIPS to container */
                    container.setCurrentAllocatedMips(mipsShareRequested);
                    container.setWorkloadMips(newMips);
                    container.changeMips(newMips);

                }
            }

            getMipsMap().put(containerUid, mipsShareAllocated);
//            getMipsMap().put(containerUid, mipsShareRequestedCapped);
//            System.out.println("Available: "+ getAvailableMips()+" Requested "+ totalRequestedMips);
            setAvailableMips(getAvailableMips()+oldMips - totalRequestedMips);
            /**Debugging */
            System.out.println("Debugging: Now total remaining MIPS of all vm pes is "+getAvailableMips());
        } else {
            redistributeMipsDueToOverSubscription();
        }

        return true;
    }

}