package org.cloudbus.cloudsim.serverless;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerBwProvisioner;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerPe;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerRamProvisioner;
import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.core.PowerContainerVm;
import org.cloudbus.cloudsim.container.schedulers.ContainerScheduler;
import org.cloudbus.cloudsim.core.CloudSim;

import java.util.*;

public class ServerlessInvoker extends PowerContainerVm {
    /**
     * The container type map of vm - contains the function type and the container list
     */
    private  Map<String, ArrayList<Container>> functionContainerMap = new HashMap<String, ArrayList<Container>>();
    /**
     * The pending container type map of vm - contains the function type and the pending container list
     */
    private  Map<String, ArrayList<Container>> functionContainerMapPending = new HashMap<String, ArrayList<Container>>();

    /**
     * The task type map of vm - contains the function type and the task count
     */

    private Map<String, Integer> vmTaskMap = new HashMap<>();
    /**
     * Records the requests in execution as per the execution order
     */

    private Stack<ServerlessRequest> runningrequestStack = new Stack<ServerlessRequest>();
    private  ArrayList<ServerlessRequest> runningrequestList = new ArrayList<>();

    /**
     * The task  map of vm - contains the function type and the task
     */
    private Map<String, ArrayList<ServerlessRequest>> vmTaskExecutionMap = new HashMap<>();
    private Map<String, ArrayList<ServerlessRequest>> vmTaskExecutionMapFull = new HashMap<>();

    /**
     * On Off status of VM
     */

    private  String status = null;

    /**
     * On Off status record time of VM
     */

    private  double recordTime = 0;

    public double onTime = 0;
    public double offTime  = 0;
    public boolean used = false;



    public ServerlessInvoker(int id, int userId, double mips, float ram, long bw, long size, String vmm, ContainerScheduler containerScheduler, ContainerRamProvisioner containerRamProvisioner, ContainerBwProvisioner containerBwProvisioner, List<? extends ContainerPe> peList, double schedulingInterval) {
        super(id, userId, mips, ram, bw, size, vmm, containerScheduler, containerRamProvisioner, containerBwProvisioner, peList, schedulingInterval);
    }

    public Map<String, Integer> getvmTaskMap(){return vmTaskMap;}

    public Map<String, ArrayList<ServerlessRequest>> getVmTaskExecutionMap(){return vmTaskExecutionMap;}
    public Map<String, ArrayList<ServerlessRequest>> getVmTaskExecutionMapFull(){return vmTaskExecutionMapFull;}
    public String getStatus(){
        return status;
    }
    public ArrayList<ServerlessRequest> getRunningRequestList() {
        return runningrequestList;
    }

    public void setStatus(String vmStatus){
        status = vmStatus;
    }

    public void setRecordTime(double time){
        recordTime = time;
    }

    public double getRecordTime(){
        return recordTime;
    }

    public void setFunctionContainerMap(Container container, String functionId){
        //System.out.println("Debug: Before: Set map "+ this.getId()+" "+functionContainerMap);

        if(!functionContainerMap.containsKey(functionId)){
            ArrayList<Container> containerListMap = new ArrayList<>();
            containerListMap.add(container);
            functionContainerMap.put(functionId,containerListMap );
        }
        else {
            if(!functionContainerMap.get(functionId).contains(container)){
                functionContainerMap.get(functionId).add(container);
            }


        }
    }
    public void setFunctionContainerMapPending(Container container, String functionId){
        //System.out.println("Debug: Before: Set map "+ this.getId()+" "+functionContainerMap);

        if(!functionContainerMapPending.containsKey(functionId)){
            ArrayList<Container> containerListMap = new ArrayList<>();
            containerListMap.add(container);
            functionContainerMapPending.put(functionId,containerListMap );
        }
        else {
            if(!functionContainerMapPending.get(functionId).contains(container)){
                functionContainerMapPending.get(functionId).add(container);
            }


        }
    }

//    @Override
    /** Insert the policy for selecting a VM when container concurrency is enableld **/
    public boolean isSuitableForContainer(Container container, ServerlessInvoker vm) {

        return (((ServerlessContainerScheduler)getContainerScheduler()).isSuitableForContainer(container, vm)
                && getContainerRamProvisioner().isSuitableForContainer(container, container.getCurrentRequestedRam()) && getContainerBwProvisioner()
                .isSuitableForContainer(container, container.getCurrentRequestedBw()));
    }

    public void addToVmTaskExecutionMap(ServerlessRequest task, ServerlessInvoker vm){

        /** Adding to full task map **/
        int countFull = vmTaskExecutionMapFull.containsKey(task.getRequestFunctionId()) ? (vmTaskExecutionMapFull.get(task.getRequestFunctionId())).size(): 0;

        if(countFull == 0){
            ArrayList<ServerlessRequest> taskListFull = new ArrayList<>();
            taskListFull.add(task);
            vmTaskExecutionMapFull.put(task.getRequestFunctionId(),taskListFull);
        }
        else{
            vmTaskExecutionMapFull.get(task.getRequestFunctionId()).add(task);
        }

        /** Adding to moving average task map **/

        int count = vmTaskExecutionMap.containsKey(task.getRequestFunctionId()) ? (vmTaskExecutionMap.get(task.getRequestFunctionId())).size(): 0;
        //vm.getVmTaskExecutionMap().put(task.getrequestFunctionId(), count+1);

        if(count == 0){
            ArrayList<ServerlessRequest> taskList = new ArrayList<>();
            taskList.add(task);
            vmTaskExecutionMap.put(task.getRequestFunctionId(),taskList);
        }
        else{
            vmTaskExecutionMap.get(task.getRequestFunctionId()).add(task);
        }

        if(count ==Constants.WINDOW_SIZE){
            vmTaskExecutionMap.get(task.getRequestFunctionId()).remove(0);
        }



    }



    public Map<String, ArrayList<Container>> getFunctionContainerMap(){
        return functionContainerMap;
    }
    public Map<String, ArrayList<Container>> getFunctionContainerMapPending(){
        return functionContainerMapPending;
    }

    @Override

    public boolean containerCreate(Container container) {
        //        Log.printLine("Host: Create VM???......" + container.getId());
        if (getSize() < container.getSize()) {
            Log.printConcatLine("[ContainerScheduler.ContainerCreate] Allocation of Container #", container.getId(), " to VM #", getId(),
                    " failed by storage");
            return false;
        }

        if (!getContainerRamProvisioner().allocateRamForContainer(container, container.getCurrentRequestedRam())) {
            Log.printConcatLine("[ContainerScheduler.ContainerCreate] Allocation of Container #", container.getId(), " to VM #", getId(),
                    " failed by RAM");
            return false;
        }

        if (!getContainerBwProvisioner().allocateBwForContainer(container, container.getCurrentRequestedBw())) {
            Log.printConcatLine("[ContainerScheduler.ContainerCreate] Allocation of Container #", container.getId(), " to VM #", getId(),
                    " failed by BW");
            getContainerRamProvisioner().deallocateRamForContainer(container);
            return false;
        }


        if (!getContainerScheduler().allocatePesForContainer(container, container.getCurrentRequestedMips())) {
            Log.printConcatLine("[ContainerScheduler.ContainerCreate] Allocation of Container #", container.getId(), " to VM #", getId(),
                    " failed by MIPS");
            getContainerRamProvisioner().deallocateRamForContainer(container);
            getContainerBwProvisioner().deallocateBwForContainer(container);
            return false;
        }
        /**Debugging */
        System.out.println("Debugging: Now available MIPS of VM "+ this.getId()+" is "+getAvailableMips());

        setSize(getSize() - container.getSize());
        //System.out.println("*************************************** "+container.getId());
        getContainerList().add(container);
        container.setVm(this);
        return true;
    }

    @Override

    public void containerDestroy(Container container) {
        //Log.printLine("Vm:  Destroy Container:.... " + container.getId());
        if (container != null) {
            containerDeallocate(container);
//            Log.printConcatLine("The Container To remove is :   ", container.getId(), "Size before removing is ", getContainerList().size(), "  vm ID is: ", getId());
            getContainerList().remove(container);

            ArrayList functionsToRemove = new ArrayList();

            //System.out.println("Debug: Invoker:before: Function containermap "+ getFunctionContainerMap());
            for (Map.Entry<String, ArrayList<Container>> entry : getFunctionContainerMap().entrySet()) {
                for(int i=0; i<entry.getValue().size(); i++){
                    //System.out.println(entry.getValue().size());
                    if(entry.getValue().get(i)==container){
                        entry.getValue().remove(i);

                        if(entry.getValue().size()==0){
                            functionsToRemove.add(entry.getKey());
//                            getFunctionContainerMap().remove(entry.getKey());
                        }
                        //System.out.println("Debug: Container "+ container.getId()+" is removed from Map");
                        break;
                    }
                }

            }
            for(int x=0; x<functionsToRemove.size(); x++){
                getFunctionContainerMap().remove(functionsToRemove.get(x));
            }
            while(getFunctionContainerMap().values().remove(null)){};
            //System.out.println("Debug: Invoker:after: Function containermap "+ getFunctionContainerMap());
            Log.printLine("ContainerVm# "+getId()+" containerDestroy:......" + container.getId() + "Is deleted from the list");

//            Log.printConcatLine("Size after removing", getContainerList().size());
            while(getContainerList().contains(container)){
                Log.printConcatLine("The container", container.getId(), " is still here");
//                getContainerList().remove(container);
            }
//            container.setVm(null);
            System.out.println("Container# "+container.getId()+" is destroyed. Now redistribute");
            /*** OW ImplementATION ***/
//            ((ServerlessDatacenter)(this.getHost().getDatacenter())).reprovisionMipsToAllContainers(this);
//            ((ServerlessDatacenter)(this.getHost().getDatacenter())).reprovisionMipsToAllContainersLinux(this);
            /*** ***/
        }
    }


    @Override
    public double updateVmProcessing(double currentTime, List<Double> mipsShare) {
        /*** VM with the longest run time based on execution time of functions in execution ***/
        double longestRunTimeVm = 0;
        /**
         * The longest reaming run time of the requests running on each container
         */

        Map<Integer, Double> runTimeContainer = new HashMap<Integer, Double>();

        Map<Integer, Double> containerQueuingTime = new HashMap<Integer, Double>();

//        if(CloudSim.clock()>12){
//            System.out.println("Debug");
//        }
        double returnTime = 0;
        double CPUUtilization = 0;
        double usedMIPS = 0;
        longestRunTimeVm=0;
//        Log.printLine("Vm: update Vms Processing at " + currentTime);
        if (mipsShare != null && !getContainerList().isEmpty()) {
            double smallerTime = Double.MAX_VALUE;
//            Log.printLine("ContainerVm: update Vms Processing");
//            Log.printLine("The VM list size is:...." + getContainerList().size());

            for (Container container : getContainerList()) {
                double time = ((ServerlessContainer) container).updateContainerProcessing(currentTime, getContainerScheduler().getAllocatedMipsForContainer(container), this);

                /** If no future event, destroy this container if auto-scaling is not enabled*/
                if((time==0 || time == Double.MAX_VALUE) && !Constants.functionAutoScaling){
                    if (!((ServerlessContainer) container).newContainer && !((ServerlessContainer) container).getIdling()) {
                            ((ServerlessDatacenter)(this.getHost().getDatacenter())).getContainersToDestroy().add(container);
                            if (Constants.containerIdlingEnabled) {
                                ((ServerlessContainer) container).setIdleStartTime(CloudSim.clock());
                                ((ServerlessContainer) container).setIdling(true);
                            }



                    }
//                    ((ServerlessDatacenter)(this.getHost().getDatacenter())).getContainersToDestroy().add(container);
//                    ((ServerlessContainer) container).setIdleStartTime(CloudSim.clock());
//                    System.out.println(CloudSim.clock()+" Debug:While processing destroy container "+container.getId());
                }
                if (time > 0.0 && time < smallerTime) {
                    smallerTime = time;
                }

                /** Updates the longest remaining run time of each vm and each container in the vms */
                double longestTime = ((ServerlessRequestScheduler) container.getContainerCloudletScheduler()).getLongestRunTime();
                runTimeContainer.put(container.getId(),longestTime);
                containerQueuingTime.put(container.getId(),((ServerlessRequestScheduler) container.getContainerCloudletScheduler()).getContainerQueueTime());


                if(longestTime>longestRunTimeVm){
                    longestRunTimeVm = longestTime;
                }
            }
            ((ServerlessDatacenter)(this.getHost().getDatacenter())).getRunTimeVm().put(this.getId(),longestRunTimeVm);

            /**Update CPU Utilization of Vm */
//            addToCPUUtilizationLog(this.getAvailableMips()/this.getTotalMips());

//            Log.printLine("ContainerVm: The Smaller time is:......" + smallerTime);

            returnTime= smallerTime;
        }
//        if (mipsShare != null) {
//            return getContainerScheduler().updateVmProcessing(currentTime, mipsShare);
//        }
        else
            returnTime= 0.0;

        if (currentTime > getPreviousTime() && (currentTime - 0.2) % getSchedulingInterval() == 0) {
            double utilization = 0;

            for (Container container : getContainerList()) {
                // The containers which are going to migrate to the vm shouldn't be added to the utilization
                if(!getContainersMigratingIn().contains(container)) {
                    returnTime = container.getContainerCloudletScheduler().getPreviousTime();
                    utilization += container.getTotalUtilizationOfCpu(returnTime);
                }
            }

            if (CloudSim.clock() != 0 || utilization != 0) {
                addUtilizationHistoryValue(utilization);
            }
            setPreviousTime(currentTime);
        }
        return returnTime;
    }

    public boolean reallocateResourcesForContainer(Container container, int cpuChange, int memChange){
        /*float newRam = container.getCurrentAllocatedRam()+Constants.RAM_INCREMENT;
        double newMIPS = newRam / this.getRam() * this.getTotalMips();*/
        System.out.println("Container #"+container.getId()+" originally has "+container.getMips()+" MIPS and "+container.getRam()+" ram");

        float newRam = (float)(container.getRam()+ memChange);
        double newMIPS= container.getMips() + cpuChange;
        System.out.println("Vm Available ram : "+getContainerRamProvisioner().getAvailableVmRam()+" Requested new additional ram for container: "+memChange);
        System.out.println("Vm Available  MIPS : "+this.getAvailableMips()+" Requested additional MIPS: "+ cpuChange*container.getNumberOfPes());

        if(container.getMips()+cpuChange>this.getMips()){
            //System.out.println(CloudSim.clock()+ " Debug:Container has reached max MIPS of "+ this.getMips());
            /*if(container.getId()==5){
                System.out.println("Debug");
            }*/
            return false;
        }
        else {


            if (!(getContainerRamProvisioner().getAvailableVmRam() >= memChange)) {

                // System.out.println("Available ram : "+getContainerRamProvisioner().getAvailableVmRam()+" Requested ram: "+newRam+"  Thus Vm ram not enough to reallocate");
                return false;
            }
            if (!(this.getAvailableMips() >= cpuChange*container.getNumberOfPes())) {
                System.out.println("Vm MIPS not enough to reallocate");
                return false;
            }
            if (!getContainerRamProvisioner().allocateRamForContainer(container, newRam)) {
                Log.printConcatLine("[ContainerAllocator.reallocateResources] Reallocation of resources to Container #", container.getId(), " in VM #", getId(),
                        " failed by RAM");
                return false;
            }

            if (!((ServerlessContainerScheduler) getContainerScheduler()).reAllocatePesForContainer(container, newMIPS)) {
                Log.printConcatLine("[ContainerScheduler.ContainerCreate] Allocation of Container #", container.getId(), " to VM #", getId(),
                        " failed by MIPS");


                return false;
            }

            System.out.println("Debugging: Reallocated resources to container. Now available MIPS of VM #" + this.getId() + " is " + getAvailableMips());
            container.setRam(Math.round(newRam));
            ((ServerlessRequestScheduler) container.getContainerCloudletScheduler()).setTotalMips(newMIPS);
            return true;
        }
    }

}
