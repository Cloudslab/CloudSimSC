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

    public ServerlessCloudletScheduler(double mips, int numberOfPes) {
        super(mips,numberOfPes);

    }

    public double cloudletSubmit(Cloudlet cl, double fileTransferTime, ServerlessInvoker vm) {
        //System.out.println("Cloudlet scheduler:CLoudlet submit : total MIPS of container "+getTotalMips());
        ResCloudlet rcl = new ResCloudlet(cl);
        if ((currentCpus - usedPes) >= cl.getNumberOfPes()) {
            rcl.setCloudletStatus(Cloudlet.INEXEC);
            //vm.getRunningCloudletStack().push((ServerlessTasks)cl);
            boolean added = false;
            for(int x=0; x< vm.getRunningCloudletList().size(); x++){
                if((((ServerlessTasks) cl).getArrivalTime()+((ServerlessTasks) cl).getMaxExecTime()<=vm.getRunningCloudletList().get(x).getArrivalTime()+vm.getRunningCloudletList().get(x).getMaxExecTime())){
                    vm.getRunningCloudletList().add(x,((ServerlessTasks) cl));
                    added = true;
                    break;
                }
            }
            if(added == false){
                vm.getRunningCloudletList(). add((ServerlessTasks) cl);
            }
            for (int i = 0; i < cl.getNumberOfPes(); i++) {
                rcl.setMachineAndPeId(0, i);
            }
            getCloudletExecList().add(rcl);
            //System.out.println("Cloudlet "+cl.getCloudletId()+" is added to exec list of container "+ ((ServerlessTasks) cl).getContainerId());;
            usedPes += cl.getNumberOfPes();
            vm.addToVmTaskExecutionMap((ServerlessTasks)cl,vm);
        } else {// no enough free PEs: go to the waiting queue

            /** Cloudlet waits till the current one finishes*/
            rcl.setCloudletStatus(Cloudlet.QUEUED);
            getCloudletWaitingList().add(rcl);
            return 0.0;
        }



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
            if(rgl.getCloudletId()==622){
                System.out.println(CloudSim.clock()+" cloudlet #622 finished");
            }
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
}
