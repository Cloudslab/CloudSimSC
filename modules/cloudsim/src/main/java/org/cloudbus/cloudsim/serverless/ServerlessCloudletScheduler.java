package org.cloudbus.cloudsim.serverless;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Consts;
import org.cloudbus.cloudsim.ResCloudlet;
import org.cloudbus.cloudsim.container.schedulers.ContainerCloudletSchedulerDynamicWorkload;
import org.cloudbus.cloudsim.core.CloudSim;

import java.util.ArrayList;
import java.util.List;

public class ServerlessCloudletScheduler extends ContainerCloudletSchedulerDynamicWorkload {

    private double longestRunTimeContainer = 0;
    private double containerQueueTime = 0;
    protected int currentCpus=0;
    protected int usedPes=0;
    /** The total current mips requested from each pe by all cloudlets allocated to this container. */
    private List<Double> totalCurrentRequestedMipsShareForCloudlets ;
    /** The total current mips allocated to all cloudlets running in this container from each pe. */
    private List<Double> totalCurrentAllocatedMipsShareForCloudlets;

    /** The total current ram requested by all cloudlets allocated to this container. */
    private double totalCurrentRequestedRamForCloudlets ;
    /** The total current ram allocated to all cloudlets running in this container. */
    private double totalCurrentAllocatedRamForCloudlets;

    public ServerlessCloudletScheduler(double mips, int numberOfPes) {
        super(mips,numberOfPes);

    }
    public boolean setTotalCurrentRequestedMipsShareForCloudlets(ServerlessTasks cl) {
        int assignedPes = 0;
        int x = 0;
        List<Double> currentRequested = totalCurrentRequestedMipsShareForCloudlets;
        while (assignedPes < cl.getNumberOfPes()){
            if (cl.getUtilizationOfCpu() <= (1 - currentRequested.get(x))){
                assignedPes ++;
                totalCurrentRequestedMipsShareForCloudlets.add(x, currentRequested.get(x) + cl.getUtilizationOfCpu());
            }
            x++;
        }

        return assignedPes == cl.getNumberOfPes();
    }
    public boolean setTotalCurrentAllocatedMipsShareForCloudlets(ServerlessTasks cl) {
        int assignedPes = 0;
        int x = 0;
        List<Double> currentAllocated = totalCurrentAllocatedMipsShareForCloudlets;
        while (assignedPes < cl.getNumberOfPes()){
            if (cl.getUtilizationOfCpu() <= (1 - currentAllocated.get(x))){
                assignedPes ++;
                totalCurrentAllocatedMipsShareForCloudlets.add(x, currentAllocated.get(x) + cl.getUtilizationOfCpu());
            }
            x++;
        }

        return assignedPes == cl.getNumberOfPes();
    }
    public boolean setTotalCurrentRequestedRamForCloudlets(ServerlessTasks cl, ServerlessContainer cont) {
        double currentRequested = totalCurrentRequestedRamForCloudlets;
        if (cl.getcloudletMemory() <= cont.getRam() - currentRequested){
            totalCurrentRequestedRamForCloudlets += cl.getcloudletMemory();
            return true;
        }
        else{
            return false;
        }

    }

    public boolean setTotalCurrentAllocatedRamForCloudlets(ServerlessTasks cl, ServerlessContainer cont) {
        double currentAllocated = totalCurrentAllocatedRamForCloudlets;
        if (cl.getcloudletMemory() <= cont.getRam() - currentAllocated){
            totalCurrentAllocatedRamForCloudlets += cl.getcloudletMemory();
            return true;
        }
        else{
            return false;
        }

    }

    public List<Double> getTotalCurrentRequestedMipsShareForCloudlets() {
        return totalCurrentRequestedMipsShareForCloudlets;
    }
    public List<Double> getTotalCurrentAllocatedMipsShareForCloudlets() {
        return totalCurrentAllocatedMipsShareForCloudlets;
    }
    public double getTotalCurrentAllocatedRamForCloudlets() {
        return totalCurrentAllocatedRamForCloudlets;
    }
    @Override
//    allocated mips to be calculated using no of pes allocated for the cloudlet (not to the entire container) * utilization % of cloudlet
    public double getEstimatedFinishTime(ResCloudlet rcl, double time) {
        ServerlessTasks cl =(ServerlessTasks)(rcl.getCloudlet());
        return time
                + ((rcl.getRemainingCloudletLength()) / (cl.getNumberOfPes()*cl.getUtilizationOfCpu()));
    }

    public double getLongestRunTime() {
        return longestRunTimeContainer;
    }

    public double getContainerQueueTime() {
        return containerQueueTime;
    }

//    Is called each time a cloudlet is finally submitted to DC
    public double cloudletSubmit(ServerlessTasks cl, ServerlessInvoker vm, ServerlessContainer cont) {
        if (!Constants.container_concurrency){
            setTotalCurrentAllocatedMipsShareForCloudlets(cl);
            setTotalCurrentAllocatedRamForCloudlets(cl, cont);
        }
        ResCloudlet rcl = new ResCloudlet(cl);
        rcl.setCloudletStatus(Cloudlet.INEXEC);
        vm.getRunningCloudletList(). add((ServerlessTasks) cl);
        rcl.setCloudletStatus(Cloudlet.INEXEC);
        vm.getRunningCloudletList(). add((ServerlessTasks) cl);
        //vm.getRunningCloudletStack().push((ServerlessTasks)cl);
//            boolean added = false;
//            for(int x=0; x< vm.getRunningCloudletList().size(); x++){
//                if((((ServerlessTasks) cl).getArrivalTime()+((ServerlessTasks) cl).getMaxExecTime()<=vm.getRunningCloudletList().get(x).getArrivalTime()+vm.getRunningCloudletList().get(x).getMaxExecTime())){
//                    vm.getRunningCloudletList().add(x,((ServerlessTasks) cl));
//                    added = true;
//                    break;
//                }
//            }
//            if(added == false){
//                vm.getRunningCloudletList(). add((ServerlessTasks) cl);
//            }
        for (int i = 0; i < cl.getNumberOfPes(); i++) {
            rcl.setMachineAndPeId(0, i);
        }
        getCloudletExecList().add(rcl);
        //System.out.println("Cloudlet "+cl.getCloudletId()+" is added to exec list of container "+ ((ServerlessTasks) cl).getContainerId());;
        usedPes += cl.getNumberOfPes();
        vm.addToVmTaskExecutionMap((ServerlessTasks)cl,vm);
        return getEstimatedFinishTime(rcl, getPreviousTime());
    }

    public double updateContainerProcessing(double currentTime, List<Double> mipsShare, ServerlessInvoker vm) {
        setCurrentMipsShare(mipsShare);
        int cpus=0;
        longestRunTimeContainer=0;
        containerQueueTime = 0;

        for (Double mips : mipsShare) { // count the CPUs available to the VMM
            if (mips > 0) {
                cpus++;
            }
        }

        currentCpus = cpus;
        double timeSpan = currentTime - getPreviousTime();
        double nextEvent = Double.MAX_VALUE;
        List<ResCloudlet> cloudletsToFinish = new ArrayList<>();

        for (ResCloudlet rcl : getCloudletExecList()) {
            rcl.updateCloudletFinishedSoFar((long) (timeSpan
                    * getTotalCurrentAllocatedMipsForCloudlet(rcl, getPreviousTime()) * Consts.MILLION));

        }
        if (getCloudletExecList().size() == 0 && getCloudletWaitingList().size() == 0) {

            setPreviousTime(currentTime);
            return 0.0;
        }

        int finished = 0;
        int pesFreed = 0;
        for (ResCloudlet rcl : getCloudletExecList()) {
            // finished anyway, rounding issue...
            if (rcl.getRemainingCloudletLength() == 0) { // finished: remove from the list
                cloudletsToFinish.add(rcl);
                finished++;
                pesFreed+=rcl.getNumberOfPes();
            }
        }
        usedPes -=pesFreed;

        for (ResCloudlet rgl : cloudletsToFinish) {
            getCloudletExecList().remove(rgl);
            cloudletFinish(rgl);
        }

        List<ResCloudlet> toRemove = new ArrayList<ResCloudlet>();
        if (!getCloudletWaitingList().isEmpty()) {
            for (int i = 0; i < finished; i++) {
                toRemove.clear();
                for (ResCloudlet rcl : getCloudletWaitingList()) {
                    if ((currentCpus - usedPes) >= rcl.getNumberOfPes()) {
                        if(rcl.getCloudlet().getCloudletId()==815){
                            System.out.println(CloudSim.clock()+" cloudlet #815 running: Debug");
                        }
                        rcl.setCloudletStatus(Cloudlet.INEXEC);
                        //vm.getRunningCloudletStack().push((ServerlessTasks) rcl.getCloudlet());
                        boolean added = false;
                        for(int x=0; x< vm.getRunningCloudletList().size(); x++){
                            if((((ServerlessTasks) rcl.getCloudlet()).getArrivalTime()+((ServerlessTasks) rcl.getCloudlet()).getMaxExecTime()<=vm.getRunningCloudletList().get(x).getArrivalTime()+vm.getRunningCloudletList().get(x).getMaxExecTime())){
                                vm.getRunningCloudletList().add(x,((ServerlessTasks) rcl.getCloudlet()));
                                added = true;
                                break;
                            }
                        }
                        if(added == false){
                            vm.getRunningCloudletList(). add((ServerlessTasks) rcl.getCloudlet());
                        }
                        for (int k = 0; k < rcl.getNumberOfPes(); k++) {
                            rcl.setMachineAndPeId(0, i);
                        }
                        getCloudletExecList().add(rcl);

                        /** To enable average latency of application */
                        vm.addToVmTaskExecutionMap((ServerlessTasks)rcl.getCloudlet(),vm);
                        usedPes += rcl.getNumberOfPes();
                        toRemove.add(rcl);
                        break;
                    }
                }
                getCloudletWaitingList().removeAll(toRemove);
            }
        }


        for (ResCloudlet rcl : getCloudletExecList()) {
            double estimatedFinishTime = getEstimatedFinishTime(rcl, currentTime);
            /*if (estimatedFinishTime - currentTime < CloudSim.getMinTimeBetweenEvents()) {
                estimatedFinishTime = currentTime + CloudSim.getMinTimeBetweenEvents();
            }*/
            if (estimatedFinishTime < nextEvent) {
                nextEvent = estimatedFinishTime;
            }

            ServerlessTasks task = (ServerlessTasks)(rcl.getCloudlet());
            /** Record the longest remaining execution time of the container*/
            containerQueueTime += task.getMaxExecTime()+ task.getArrivalTime()-CloudSim.clock();
            if (task.getMaxExecTime()+ task.getArrivalTime()-CloudSim.clock()> longestRunTimeContainer) {
                longestRunTimeContainer = task.getMaxExecTime()+ task.getArrivalTime()-CloudSim.clock();
            }
        }

        for (ResCloudlet rcl : getCloudletWaitingList()) {
            ServerlessTasks task = (ServerlessTasks)(rcl.getCloudlet());
            containerQueueTime += task.getMaxExecTime();
            /** Record the longest remaining execution time of the container*/
            if (task.getMaxExecTime()+ task.getArrivalTime()-CloudSim.clock()> longestRunTimeContainer) {
                longestRunTimeContainer = task.getMaxExecTime()+ task.getArrivalTime()-CloudSim.clock();
            }

        }




        setPreviousTime(currentTime);


        cloudletsToFinish.clear();

        return nextEvent;
    }

    @Override
    public Cloudlet cloudletCancel(int cloudletId) {
        boolean found = false;
        int position = 0;

        // First, looks in the finished queue
        for (ResCloudlet rcl : getCloudletFinishedList()) {
            if (rcl.getCloudletId() == cloudletId) {
                found = true;
                break;
            }
            position++;
        }

        if (found) {
            return getCloudletFinishedList().remove(position).getCloudlet();
        }

        // Then searches in the exec list
        position = 0;
        found = false;
        for (ResCloudlet rcl : getCloudletExecList()) {
            if (rcl.getCloudletId() == cloudletId) {
                found = true;
                break;
            }
            position++;
        }

        if (found) {
            ResCloudlet rcl = getCloudletExecList().remove(position);
            if (rcl.getRemainingCloudletLength() == 0) {
                cloudletFinish(rcl);
            } else {
                rcl.setCloudletStatus(Cloudlet.CANCELED);
            }
            return rcl.getCloudlet();
        }


        // Then searches in the waiting list
        position = 0;
        found = false;
        for (ResCloudlet rcl : getCloudletWaitingList()) {
            if (rcl.getCloudletId() == cloudletId) {
                found = true;
                break;
            }
            position++;
        }

        if (found) {
            ResCloudlet rcl = getCloudletWaitingList().remove(position);
            if (rcl.getRemainingCloudletLength() == 0) {
                cloudletFinish(rcl);
            } else {
                rcl.setCloudletStatus(Cloudlet.CANCELED);
            }
            return rcl.getCloudlet();
        }

        // Now, looks in the paused queue
        found = false;
        position = 0;
        for (ResCloudlet rcl : getCloudletPausedList()) {
            if (rcl.getCloudletId() == cloudletId) {
                found = true;
                rcl.setCloudletStatus(Cloudlet.CANCELED);
                break;
            }
            position++;
        }

        if (found) {
            return getCloudletPausedList().remove(position).getCloudlet();
        }

        return null;
    }
}
