package org.cloudbus.cloudsim.serverless;

import javafx.util.Pair;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.ResCloudlet;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.container.core.*;
import org.cloudbus.cloudsim.container.lists.ContainerList;
import org.cloudbus.cloudsim.container.resourceAllocators.ContainerAllocationPolicy;
import org.cloudbus.cloudsim.container.resourceAllocators.ContainerVmAllocationPolicy;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;

import java.util.*;

public class ServerlessDatacenter extends PowerContainerDatacenterCM {

    /**
     * Cloudlets to be rescheduled
     */
    private final Map<Integer, ServerlessTasks> tasksWaitingToReschedule;

    /**
     * Idle Vm list
     */
    private static List<ServerlessInvoker> vmIdleList = new ArrayList<>();
    /**
     * Cloudlet reschedule event
     */
    private boolean reschedule = false;
    /**
     * The longest reaming run time of the cloudlets running on each vm
     */

    private static Map<Integer, Double> runTimeVm = new HashMap<Integer, Double>();

    /**
     * The bin of vms against their longest run time
     */
    private static Map<Integer, ArrayList<Integer>> onlineBinOfVms = new HashMap<Integer, ArrayList<Integer>>();
    /**
     * The bin of vms against their longest run time
     */
    private static ArrayList<Integer> binNos = new ArrayList<>();
    /**
     * Perform Datacenter monitoring
     */
    private boolean monitoring = false;
    /**
     * Idle containers that need to be destroyed
     */
    private List<Container> containersToDestroy = new ArrayList<>();
    /**
     * CLoudlet submit event of DC event
     */
    protected boolean DC_event;

    /**
     * The load balancerfor DC.
     */
    private RequestLoadBalancer requestLoadBalancer;

    /**
     * Allocates a new PowerDatacenter object.
     *
     * @param name
     * @param characteristics
     * @param vmAllocationPolicy
     * @param containerAllocationPolicy
     * @param storageList
     * @param schedulingInterval
     * @param experimentName
     * @param logAddress
     * @throws Exception
     */


    public ServerlessDatacenter(String name, ContainerDatacenterCharacteristics characteristics, ContainerVmAllocationPolicy vmAllocationPolicy, ContainerAllocationPolicy containerAllocationPolicy, List<Storage> storageList, double schedulingInterval, String experimentName, String logAddress, double vmStartupDelay, double containerStartupDelay, boolean monitor) throws Exception {
        super(name, characteristics, vmAllocationPolicy, containerAllocationPolicy, storageList, schedulingInterval, experimentName, logAddress, vmStartupDelay, containerStartupDelay);
        tasksWaitingToReschedule = new HashMap<Integer, ServerlessTasks>();
        setMonitoring(monitor);
//        setRequestLoadBalancerR(loadBalancer);

    }

    /**
     * Get the idle containers that need to be destroyed
     */
    public List<Container> getContainersToDestroy() {
        return containersToDestroy;
    }

    public RequestLoadBalancer getRequestLoadBalancer() {
        return requestLoadBalancer;
    }

    public Map<Integer,Double> getRunTimeVm(){
        return runTimeVm;
    }

    public boolean getMonitoring() {
        return monitoring;
    }

    public void setMonitoring(boolean monitor) {
        this.monitoring = monitor;
    }
    public void setRequestLoadBalancerR(RequestLoadBalancer lb) {
        this.requestLoadBalancer = lb;
    }



    @Override
    protected void processOtherEvent(SimEvent ev) {
        switch (ev.getTag()) {
            case CloudSimTags.DEADLINE_CHECKPOINT:
                processDeadlineCheckpoint(ev, false);
                break;
            case CloudSimTags.CONTAINER_DESTROY:
                processContainerDestroy(ev, false);
                break;

            case CloudSimTags.CONTAINER_DESTROY_ACK:
                processContainerDestroy(ev, true);
                break;

            case containerCloudSimTags.CONTAINER_SUBMIT_FOR_RESCHEDULE:
                ((ServerlessContainer) ev.getData()).setReschedule(true);
                processContainerSubmit(ev, true);
                break;

            case CloudSimTags.PREEMPT_CLOUDLET:
                preemptCloudlet(ev);
                break;


            // other unknown tags are processed by this method
            default:
                super.processOtherEvent(ev);
                break;
        }
    }

    public void preemptCloudlet(SimEvent ev){
        ServerlessTasks cl = (ServerlessTasks) ev.getData();
        ContainerHost host = getVmAllocationPolicy().getHost(cl.getVmId(),cl.getUserId() );
        ServerlessInvoker vm = (ServerlessInvoker) host.getContainerVm(cl.getVmId(),cl.getUserId());
        Container container = vm.getContainer(cl.getContainerId(), cl.getUserId());
        if (container != null) {
            double remainingLength = cl.getCloudletLength()-cl.getCloudletFinishedSoFar();
            if (remainingLength > 10) {

                System.out.println("Cloudlet" + cl.getCloudletId() +" in container "+ cl.getContainerId()+" is preempted");
                Cloudlet returnTask = getVmAllocationPolicy().getHost(cl.getVmId(), cl.getUserId()).getContainerVm(cl.getVmId(), cl.getUserId()).getContainer(((ServerlessTasks) cl).getContainerId(), cl.getUserId())
                        .getContainerCloudletScheduler().cloudletCancel(cl.getCloudletId());

                sendNow(this.getId(), CloudSimTags.CONTAINER_DESTROY_ACK,container);
            }
        }

    }

    /** Process event for deadline checkpointing */
    public void processDeadlineCheckpoint(SimEvent ev, boolean ack){
        /*if(CloudSim.clock()==11.251999999999999){
            System.out.println("Debug");
        }*/

        updateCloudletProcessing();
        ServerlessTasks cl = (ServerlessTasks) ev.getData();
        if(cl.getStatus()==3) {
            ContainerHost host = getVmAllocationPolicy().getHost(cl.getVmId(),cl.getUserId() );
            ServerlessInvoker vm = (ServerlessInvoker) host.getContainerVm(cl.getVmId(),cl.getUserId());
            Container container = vm.getContainer(cl.getContainerId(), cl.getUserId());
            // System.out.println(CloudSim.clock()+" Debug:DC: Cloudlet's container is "+ container);

//        double timeSpan = CloudSim.clock() - (container.getContainerCloudletScheduler()).getPreviousTime();

//        double remainingLength = cl.getCloudletLength()-(cl.getCloudletFinishedSoFar()+ (((ServerlessCloudletScheduler) (container.getContainerCloudletScheduler())).getTotalMips())*timeSpan);

            double remainingLength = cl.getCloudletLength()-cl.getCloudletFinishedSoFar();


            if (remainingLength > 1) {
                System.out.println(CloudSim.clock()+" Debug:DC: Cloudlet #"+cl.getCloudletId()+" has not finished > Reschedule");

//            if(vm.getTotalMips())
                double vmCPUUsageBefore=1 - vm.getAvailableMips() / vm.getTotalMips();
                //System.out.println(CloudSim.clock()+" vmCPUUsageBefore: "+vmCPUUsageBefore);
                if(vmCPUUsageBefore <= Constants.VM_CPU_USAGE_THRESHOLD) {
                    boolean result = ((FunctionScheduler) getContainerAllocationPolicy()).reallocateVmResourcesForContainer(container, vm,cl);
                    if (result) {
//                    updateCloudletProcessing();
//                    System.out.println(container.getMips());
                        double estimatedFinishTime = remainingLength / (container.getMips());
                        double delay = 0;
                        if(cl.getPriority()==1){
                            delay = (cl.getArrivalTime() + cl.getMaxExecTime() - CloudSim.clock()) * Constants.DEADLINE_CHECKPOINT_LOW;
                        }
                        else{
                            delay = (cl.getArrivalTime() + cl.getMaxExecTime() - CloudSim.clock()) * Constants.DEADLINE_CHECKPOINT_HIGH;
                        }
                        if (delay > 1) {
                            send(getId(), delay, CloudSimTags.DEADLINE_CHECKPOINT, cl);
                        }
//                        send(getId(), ((cl.getArrivalTime() + cl.getMaxExecTime() - CloudSim.clock()) * Constants.DEADLINE_CHECKPOINT), CloudSimTags.DEADLINE_CHECKPOINT, cl);
                        send(getId(), estimatedFinishTime, CloudSimTags.VM_DATACENTER_EVENT);

                        System.out.println(CloudSim.clock() + " Debug:DC: Container #" + container.getId() + " now has: " + container.getRam() + " ram and " + container.getMips() + " MIPS");
                        /** Check now if the node is constrained */
                        double vmCPUUsageAfter = 1 - vm.getAvailableMips() / vm.getTotalMips();
                        System.out.println(CloudSim.clock() + " Debug:DC: vm #" + vm.getId() +" vmCPUUsageAfter: "+vmCPUUsageAfter);





                        ArrayList<ResCloudlet> toRemove = new ArrayList<ResCloudlet>();
                        double mipsFreed = 0;
                        //System.out.println("Cloudlet Stack!: " + vm.getRunningCloudletStack());
                        for (ServerlessTasks task : vm.getRunningCloudletList()) {
                            System.out.println(task.getCloudletId() +" "+(task.getArrivalTime()+task.getMaxExecTime())+ ", ");
                        }
                        if(vm.getRunningCloudletList().size()>0) {
                            while (vmCPUUsageAfter > Constants.VM_CPU_USAGE_THRESHOLD) {

                                for (int i = vm.getRunningCloudletList().size() - 1; i >= 0; i--) {

                                    ServerlessTasks toReschedule = vm.getRunningCloudletList().remove(i);
                                    //System.out.println("Cloudlet: " + toReschedule.getCloudletId() + " SPent time: " + (CloudSim.clock() - toReschedule.getArrivalTime()) + " Available time: " + toReschedule.getMaxExecTime() * Constants.DEADLINE_CHECKPOINT);
                                    ServerlessContainer cloudletCont = ContainerList.getById(getContainerList(), toReschedule.getContainerId());
                                    if(toReschedule.getCloudletId()==1253 && toReschedule.reschedule==true){
                                        System.out.println("my reschedule status "+ toReschedule.reschedule);
                                    }
                                    if(cloudletCont!=null) {
                                        if ((CloudSim.clock() - toReschedule.getArrivalTime() + Constants.FUNCTION_SCHEDULING_DELAY) < toReschedule.getMaxExecTime() * 0.2 && cloudletCont.getContainerCloudletScheduler().getCloudletWaitingList().isEmpty() && toReschedule.reschedule!=true && toReschedule.getPriority()==2) {

                                            System.out.println(CloudSim.clock() + " Debug:DC: Evict cloudlet #" + toReschedule.getCloudletId() + " in container " + cloudletCont.getId());
                                            //System.out.println(CloudSim.clock() + " Debug:DC: Container #" + cloudletCont.getId() + " execution list is " + cloudletCont.getContainerCloudletScheduler().getCloudletExecList() + " and waiting list is " + cloudletCont.getContainerCloudletScheduler().getCloudletWaitingList());
                                            while (!cloudletCont.getContainerCloudletScheduler().getCloudletExecList().isEmpty() || !cloudletCont.getContainerCloudletScheduler().getCloudletWaitingList().isEmpty()) {
                                                for (ResCloudlet rcl : cloudletCont.getContainerCloudletScheduler().getCloudletExecList()) {
                                                    rescheduleCloudlet((ServerlessTasks) rcl.getCloudlet(),cloudletCont);
                                                    toReschedule.setReschedule(true);
                                                    System.out.println("my reschedule status after"+ toReschedule.reschedule);
                                                    toRemove.add(rcl);
                                                }

                                                for (ResCloudlet rcl : cloudletCont.getContainerCloudletScheduler().getCloudletWaitingList()) {
                                                    rescheduleCloudlet((ServerlessTasks) rcl.getCloudlet(), cloudletCont);
                                                    toReschedule.setReschedule(true);
                                                    System.out.println("my reschedule status after"+ toReschedule.reschedule);
                                                    toRemove.add(rcl);
                                                }

                                                for (int x = 0; x < toRemove.size(); x++) {
                                                    Cloudlet remove = toRemove.get(x).getCloudlet();
                                                    Cloudlet returnTask = getVmAllocationPolicy().getHost(remove.getVmId(), remove.getUserId()).getContainerVm(remove.getVmId(), remove.getUserId()).getContainer(((ServerlessTasks) remove).getContainerId(), remove.getUserId())
                                                            .getContainerCloudletScheduler().cloudletCancel(remove.getCloudletId());
                                                }
//                                cloudletCont.getContainerCloudletScheduler().getCloudletExecList().removeAll(toRemove);

//                                cloudletCont.getContainerCloudletScheduler().getCloudletWaitingList().removeAll(toRemove);
                                                toRemove.clear();
                                            }

                                            mipsFreed += cloudletCont.getMips();
                                            vmCPUUsageAfter = 1 - (vm.getAvailableMips() + mipsFreed) / vm.getTotalMips();
                                            System.out.println(CloudSim.clock() + " Debug:DC: CPU usage is now " + vmCPUUsageAfter);

                                            //*** add cloudlet's old container to the removal list
                                            getContainersToDestroy().add(cloudletCont);
                                            //System.out.println("Container to be destroyed due to contention: " + cloudletCont.getId());
                                            System.out.println(CloudSim.clock() + " Debug:Due to rescheduling destroy container " + cloudletCont.getId());
                                            if (vmCPUUsageAfter < Constants.VM_CPU_USAGE_THRESHOLD)
                                                break;
                                        }
                                    }
                                }
                                break;

                            }
                        }





                    }
                    else {
                        System.out.println(CloudSim.clock() + " Debug:DC: Rescheduling for container #" + container.getId() + " failed");
                        double delay=0;
                        if(cl.getPriority()==1){
                            delay = ((cl.getArrivalTime() + cl.getMaxExecTime() - CloudSim.clock()) * Constants.DEADLINE_CHECKPOINT_LOW);
                        }
                        else{
                            delay = ((cl.getArrivalTime() + cl.getMaxExecTime() - CloudSim.clock()) * Constants.DEADLINE_CHECKPOINT_HIGH);
                        }
//                         delay = ((cl.getArrivalTime() + cl.getMaxExecTime() - CloudSim.clock()) * Constants.DEADLINE_CHECKPOINT);
                        if (delay > 1) {
                            send(getId(), delay, CloudSimTags.DEADLINE_CHECKPOINT, cl);
                        }
                    }
                }
                else{
                    double delay=0;
                    if(cl.getPriority()==1){
                        delay = ((cl.getArrivalTime() + cl.getMaxExecTime() - CloudSim.clock()) * Constants.DEADLINE_CHECKPOINT_LOW);
                    }
                    else{
                        delay = ((cl.getArrivalTime() + cl.getMaxExecTime() - CloudSim.clock()) * Constants.DEADLINE_CHECKPOINT_HIGH);
                    }
//                    delay = ((cl.getArrivalTime() + cl.getMaxExecTime() - CloudSim.clock()) * Constants.DEADLINE_CHECKPOINT);
                    if(delay>1) {
                        send(getId(), delay, CloudSimTags.DEADLINE_CHECKPOINT, cl);
                    }

                }
            }

        }

        /** Destroy idling containers*/
        while(!containersToDestroy.isEmpty()){
            for(int x=0; x<containersToDestroy.size(); x++){
                if(((ServerlessContainer)getContainersToDestroy().get(x)).newContainer==true){
//                    System.out.println("Container to be destroyed removed since new: "+getContainersToDestroy().get(x).getId());
                    getContainersToDestroy().remove(x);
                    continue;
                }
                //System.out.println("Container to be destroyed: "+containersToDestroy.get(x).getId());
                if(containersToDestroy.get(x).getId()==94){
                    System.out.println("to be destroyed 94 at checkpoint");
                }
                sendNow(this.getId(), CloudSimTags.CONTAINER_DESTROY_ACK,containersToDestroy.get(x));
            }
            containersToDestroy.clear();
        }


    }

    public void rescheduleCloudlet(ServerlessTasks cloudlet, ServerlessContainer container) {
        System.out.println("Debug DC: Trying to reschedule cloudlet #" + cloudlet.getCloudletId());

        /*** Updating task memory to current ***/
        cloudlet.setcloudletMemory((int) container.getCurrentAllocatedRam());
        tasksWaitingToReschedule.put(cloudlet.getCloudletId(), cloudlet);

        send(cloudlet.getUserId(), Constants.FUNCTION_SCHEDULING_DELAY, CloudSimTags.CLOUDLET_RESCHEDULE, cloudlet);

    }

    @Override
    protected void processCloudletMove(int[] receivedData, int type) {
        updateCloudletProcessing();

        int[] array = receivedData;
        int cloudletId = array[0];
        int userId = array[1];
        int vmId = array[2];
        int containerId = array[3];
        int vmDestId = array[4];
        int containerDestId = array[5];
        int destId = array[6];
        ServerlessInvoker newContainerVm = null;

        // get the cloudlet
//        Cloudlet cl = getVmAllocationPolicy().getHost(vmId, userId).getContainerVm(vmId, userId).getContainer(containerId, userId)
//                .getContainerCloudletScheduler().cloudletCancel(cloudletId);

        ServerlessTasks cl = tasksWaitingToReschedule.get(cloudletId);
        tasksWaitingToReschedule.remove(cloudletId,cl);
        ServerlessInvoker oldVm = (ServerlessInvoker) getVmAllocationPolicy().getHost(vmId, userId).getContainerVm(vmId, userId);

        /** Remove cloudlet from old vmtaskmap */
//        removeFromVmTaskMap((ServerlessTasks)cl,oldVm);



        boolean failed = false;
        if (cl == null) {// cloudlet doesn't exist
            failed = true;
        } else {
            // has the cloudlet already finished?
            if (cl.getCloudletStatusString().equals("Success")) {// if yes, send it back to user
                int[] data = new int[3];
                data[0] = getId();
                data[1] = cloudletId;
                data[2] = 0;
                sendNow(cl.getUserId(), CloudSimTags.CLOUDLET_SUBMIT_ACK, data);
                sendNow(cl.getUserId(), CloudSimTags.CLOUDLET_RETURN, cl);
            }

            // prepare cloudlet for migration
            cl.setVmId(vmDestId);
            ((ContainerCloudlet)cl).setContainerId(containerDestId);



            // the cloudlet will migrate from one vm to another does the destination VM exist?
            if (destId == getId()) {
                newContainerVm = (ServerlessInvoker) getVmAllocationPolicy().getHost(vmDestId, userId).getContainerVm(vmDestId, userId);

                /** Add cloudlet to new vmtaskmap */
//                addToVmTaskMap((ServerlessTasks)cl,newContainerVm);

                if (newContainerVm == null) {
                    failed = true;
                } else {

                    // time to transfer the files
                    double fileTransferTime = predictFileTransferTime(cl.getRequiredFiles());
                    //System.out.println("Vm "+ newContainerVm.getId()+"Cont list size: "+newContainerVm.getContainerList().size());
                    /*for(Container cont: newContainerVm.getContainerList()){
                        System.out.println(cont.getId());
                    }*/
                    ServerlessContainer newContainer = (ServerlessContainer)(getVmAllocationPolicy().getHost(vmDestId, userId).getContainerVm(vmDestId, userId).getContainer(containerDestId, userId));

                    cl.setResourceParameter(getId(), getCharacteristics().getCostPerSecond(), getCharacteristics()
                            .getCostPerBw(), cl.getVmId());
                    //System.out.println("Cloudlet scheduler!!! cont ID "+newContainer);

                    /*** set new MIPS to all containers ***/

//                    reprovisionMipsToAllContainers(newContainerVm);











                    System.out.println(("Cloudlet scheduler!!! cont ID "+newContainer.getId()+" "+(ServerlessCloudletScheduler)newContainer.getContainerCloudletScheduler()));
                    double estimatedFinishTime= ((ServerlessCloudletScheduler)newContainer.getContainerCloudletScheduler()).cloudletSubmit(cl, newContainerVm, newContainer);

                    /** Update vm bin with new cloudlet's deadline */
                    updateOnlineVmBin(newContainerVm, ((ServerlessTasks)cl).getArrivalTime()+((ServerlessTasks)cl).getMaxExecTime()-CloudSim.clock());
                    updateRunTimeVm(newContainerVm, ((ServerlessTasks)cl).getArrivalTime()+((ServerlessTasks)cl).getMaxExecTime()-CloudSim.clock());

                    /** Send an event when 90% of the deadline is reached for a cloudlet */
                    if(cl.getPriority()==1){
                        send(getId(), ((((ServerlessTasks)cl).getArrivalTime()+((ServerlessTasks)cl).getMaxExecTime()-CloudSim.clock())* Constants.DEADLINE_CHECKPOINT_LOW), CloudSimTags.DEADLINE_CHECKPOINT,cl);
                    }
                    else{
                        send(getId(), ((((ServerlessTasks)cl).getArrivalTime()+((ServerlessTasks)cl).getMaxExecTime()-CloudSim.clock())* Constants.DEADLINE_CHECKPOINT_HIGH), CloudSimTags.DEADLINE_CHECKPOINT,cl);
                    }
//                    send(getId(), ((((ServerlessTasks)cl).getArrivalTime()+((ServerlessTasks)cl).getMaxExecTime()-CloudSim.clock())* Constants.DEADLINE_CHECKPOINT), CloudSimTags.DEADLINE_CHECKPOINT,cl);

                    // if this cloudlet is in the exec queue
                    if (estimatedFinishTime > 0.0 && !Double.isInfinite(estimatedFinishTime)) {
                        estimatedFinishTime += fileTransferTime;

                        /**Remove the new cloudlet's new container from removal list */
                        getContainersToDestroy().remove(newContainer);
                        //System.out.println("CLoudlet move: Removed from destroy list container "+newContainer.getId());
//                        getContainersToDestroy().add((ServerlessContainer)newContainerVm.getContainer(containerId, userId));

                        send(getId(), (estimatedFinishTime-CloudSim.clock()), CloudSimTags.VM_DATACENTER_EVENT);
//                        send(getId(), (cl.getMaxExecTime()+cl.getArrivalTime()-CloudSim.clock()), CloudSimTags.PREEMPT_CLOUDLET);
                    }

                }

            } else {// the cloudlet will migrate from one resource to another
                int tag = ((type == CloudSimTags.CLOUDLET_MOVE_ACK) ? CloudSimTags.CLOUDLET_SUBMIT
                        : CloudSimTags.CLOUDLET_SUBMIT_ACK);
                sendNow(destId, tag, cl);
            }

        }

        checkCloudletCompletion();

        /** Destroy idling containers*/
        while(!containersToDestroy.isEmpty()){
            for(int x=0; x<containersToDestroy.size(); x++){
                if(((ServerlessContainer)containersToDestroy.get(x)).newContainer==true){
//                    System.out.println("Container to be destroyed removed since new: "+getContainersToDestroy().get(x).getId());
                    getContainersToDestroy().remove(x);
                    continue;
                }
                if(containersToDestroy.get(x).getId()==94){
                    System.out.println("to be destroyed 94 at cloudlet move");
                }
                //System.out.println("Container to be destroyed: "+containersToDestroy.get(x).getId());
                sendNow(this.getId(), CloudSimTags.CONTAINER_DESTROY_ACK,containersToDestroy.get(x));
            }
            containersToDestroy.clear();
        }

        /**Update CPU Utilization of Vm */
        /*for(ContainerVm vm: getContainerVmList()){
            addToCPUUtilizationLog(vm.getId(),vm.getAvailableMips()/vm.getTotalMips());
        }*/


        if (type == CloudSimTags.CLOUDLET_MOVE_ACK) {// send ACK if requested
            /*int[] data = new int[4];
            data[0] = getId();
            data[1] = cloudletId;
            data[2] = oldVm.getId();
            data[3] = newContainerVm.getId() ;
            if (failed) {
                data[4] = 0;
            } else {
                data[4] = 1;
            }*/

            Pair data = new Pair<>(cl,oldVm.getId());

            sendNow(cl.getUserId(), CloudSimTags.CLOUDLET_MOVE_ACK, data);
        }
    }


    /** Process event to destroy a container */
    public void processContainerDestroy(SimEvent ev, boolean ack){
        Container container = (Container) ev.getData();
        ServerlessInvoker vm = (ServerlessInvoker)container.getVm();
        if(vm!=null) {
            getContainerAllocationPolicy().deallocateVmForContainer(container);

            /** Add vm to idle list if there are no more containers */
            if ((vm.getContainerList()).size() == 0) {
                vmIdleList.add((ServerlessInvoker) container.getVm());
                if (vm.getStatus().equals("ON")) {
                    vm.setStatus("OFF");
                    vm.onTime += (CloudSim.clock() - vm.getRecordTime());
                } else if (vm.getStatus().equals("OFF")) {
//                vm.setStatus("ON");
                    vm.offTime += (CloudSim.clock() - vm.getRecordTime());
                }
                vm.setRecordTime(CloudSim.clock());
            }
            if (ack) {
                int[] data = new int[4];
                data[0] = getId();
                data[1] = container.getId();
                data[2] = CloudSimTags.TRUE;
                data[3] = vm.getId();

                sendNow(container.getUserId(), CloudSimTags.CONTAINER_DESTROY_ACK, data);
            }

            getContainerList().remove(container);

        }

    }

    @Override

    protected void processVmCreate(SimEvent ev, boolean ack) {
        ContainerVm containerVm = (ContainerVm) ev.getData();

        boolean result = getVmAllocationPolicy().allocateHostForVm(containerVm);

        if (ack) {
            int[] data = new int[3];
            data[0] = getId();
            data[1] = containerVm.getId();

            if (result) {
                data[2] = CloudSimTags.TRUE;
            } else {
                data[2] = CloudSimTags.FALSE;
            }
            send(containerVm.getUserId(), CloudSim.getMinTimeBetweenEvents(), CloudSimTags.VM_CREATE_ACK, data);

            /** Add Vm to idle list */
            vmIdleList.add((ServerlessInvoker)containerVm);
            ((ServerlessInvoker)containerVm).setStatus("OFF");
            ((ServerlessInvoker)containerVm).setRecordTime(CloudSim.clock());
        }


        /*int bin = 0;
        if(onlineBinOfVms.containsKey(bin)){
            onlineBinOfVms.get(bin).add(containerVm.getId());
        }
        else {
            ArrayList<Integer> instanceIds = new ArrayList<>();
            instanceIds.add(containerVm.getId());
            onlineBinOfVms.put(bin, instanceIds);
            binNos.add(bin);
            System.out.println(CloudSim.clock() + " Bin " + bin + " is added to binArray");
            Collections.sort(binNos);
        }*/

        runTimeVm.put(containerVm.getId(), (double) 0);

        if (result) {
            getContainerVmList().add((PowerContainerVm) containerVm);

            if (containerVm.isBeingInstantiated()) {
                containerVm.setBeingInstantiated(false);
            }

            /*((ServerlessInvoker)containerVm).getVmUpTime().put("In", (double) 0);
            ((ServerlessInvoker)containerVm).getVmUpTime().put("Out", (double) 0);
            ((ServerlessInvoker)containerVm).inTime = CloudSim.clock();
            ((ServerlessInvoker)containerVm).outTime = CloudSim.clock();*/



            containerVm.updateVmProcessing(CloudSim.clock(), getVmAllocationPolicy().getHost(containerVm).getContainerVmScheduler()
                    .getAllocatedMipsForContainerVm(containerVm));

            if(containerVm.getId()==1 && Constants.monitoring) {

                send(containerVm.getUserId(), Constants.CPU_USAGE_MONITORING_INTERVAL, CloudSimTags.RECORD_CPU_USAGE, containerVm);
            }
        }

    }

/*    public void setFunctionVmMap(ServerlessInvoker vm, String functionId){
        if(!functionVmMap.containsKey(functionId)){
            ArrayList<ServerlessInvoker> vmListMap = new ArrayList<>();
            vmListMap.add(vm);
            functionVmMap.put(functionId,vmListMap);
        }
        else{
            functionVmMap.get(functionId).add(vm);
        }
    }*/

    @Override
    public void processContainerSubmit(SimEvent ev, boolean ack) {
        Container container = (Container) ev.getData();
        boolean result = false;

        /** Directly allocate resources to container if a vm is assigned already */
        if (container.getVm() != null) {
            result = getContainerAllocationPolicy().allocateVmForContainer(container, container.getVm(), getContainerVmList());
        } else {
            result = getContainerAllocationPolicy().allocateVmForContainer(container, getContainerVmList());
        }

        if (ack) {
            int[] data = new int[4];
            data[1] = container.getId();
            if (result) {
                data[2] = CloudSimTags.TRUE;
            } else {
                data[2] = CloudSimTags.FALSE;
            }
            data[3] = 0;
            if (result) {
                ServerlessInvoker vm = ((ServerlessInvoker) container.getVm());
                if (getMonitoring()) {
                    /** Remove vm from idle list when first container is created */
                    if (vm.getContainerList().size() == 1) {
                        vmIdleList.remove(vm);
                        vm.setStatus("ON");
                        vm.offTime += (CloudSim.clock() - vm.getRecordTime());
                        vm.setRecordTime(CloudSim.clock());

                            /*double outTimeRecorded = (vm.getVmUpTime()).get("Out");
                             (vm.getVmUpTime()).put("Out",outTimeRecorded+CloudSim.clock()-vm.outTime);*/
                        //                         vm.outTime = CloudSim.clock();
                        //                         vm.inTime = CloudSim.clock();
                    }
                }
                data[0] = vm.getId();
                if (vm.getId() == -1) {

                    Log.printConcatLine("The ContainerVM ID is not known (-1) !");
                }
//                    Log.printConcatLine("Assigning the container#" + container.getUid() + "to VM #" + containerVm.getUid());
                getContainerList().add(container);
                if (container.isBeingInstantiated()) {
                    container.setBeingInstantiated(false);
                }
                /*if(container.getId()==2){
                    System.out.println("Debug");
                }*/
                ((ServerlessContainer) container).updateContainerProcessing(CloudSim.clock(), getContainerAllocationPolicy().getContainerVm(container).getContainerScheduler().getAllocatedMipsForContainer(container), vm);
                vm.setFunctionContainerMapPending(container, ((ServerlessContainer) container).getType());
                send(ev.getSource(), Constants.CONTAINER_STARTTUP_DELAY, containerCloudSimTags.CONTAINER_CREATE_ACK, data);

            }
            else {
                data[0] = -1;
                //notAssigned.add(container);
                Log.printLine(String.format("Couldn't find a vm to host the container #%s", container.getUid()));

            }

        }
    }

        @Override
        public void updateCloudletProcessing(){
            // if some time passed since last processing
            // R: for term is to allow loop at simulation start. Otherwise, one initial
            // simulation step is skipped and schedulers are not properly initialized
            if (CloudSim.clock() < 0.111 || CloudSim.clock() > getLastProcessTime()) {
                List<? extends ContainerHost> list = getVmAllocationPolicy().getContainerHostList();
                double smallerTime = Double.MAX_VALUE;
                for (ContainerHost host : list) {
                    // inform VMs to update processing
                    double time = host.updateContainerVmsProcessing(CloudSim.clock());
                    // what time do we expect that the next cloudlet will finish?
                    if (time < smallerTime) {
                        smallerTime = time;
                    }
                }

                // gurantees a minimal interval before scheduling the event
            /*if (smallerTime < CloudSim.clock() + CloudSim.getMinTimeBetweenEvents() + 0.01) {
                smallerTime = CloudSim.clock() + CloudSim.getMinTimeBetweenEvents() + 0.01;
            }*/
                if (smallerTime != Double.MAX_VALUE) {
                    schedule(getId(), (smallerTime - CloudSim.clock()), CloudSimTags.VM_DATACENTER_EVENT);
                }
                setLastProcessTime(CloudSim.clock());

                /**Destroy idling containers if this is a DC_EVENT       */
                if (DC_event) {
                    destroyIdleContainers();
                }

                /**Create online bin        */
                createOnlineVmBin();


            }
            DC_event = false;


        }

    @Override
    protected void processCloudletSubmit(SimEvent ev, boolean ack) {
        updateCloudletProcessing();

        try {
            ServerlessTasks cl = (ServerlessTasks) ev.getData();

            // checks whether this Cloudlet has finished or not
            if (cl.isFinished()) {
                String name = CloudSim.getEntityName(cl.getUserId());
                Log.printConcatLine(getName(), ": Warning - Cloudlet #", cl.getCloudletId(), " owned by ", name,
                        " is already completed/finished.");
                Log.printLine("Therefore, it is not being executed again");
                Log.printLine();

                // NOTE: If a Cloudlet has finished, then it won't be processed.
                // So, if ack is required, this method sends back a result.
                // If ack is not required, this method don't send back a result.
                // Hence, this might cause CloudSim to be hanged since waiting
                // for this Cloudlet back.
//                if (ack) {
                int[] data = new int[3];
                data[0] = getId();
                data[1] = cl.getCloudletId();

                // unique tag = operation tag
                int tag = CloudSimTags.CLOUDLET_SUBMIT_ACK;
                sendNow(cl.getUserId(), tag, data);
//                }

                sendNow(cl.getUserId(), CloudSimTags.CLOUDLET_RETURN, cl);

                return;
            }

            // process this Cloudlet to this CloudResource
            cl.setResourceParameter(getId(), getCharacteristics().getCostPerSecond(), getCharacteristics()
                    .getCostPerBw(), cl.getVmId());

            int userId = cl.getUserId();
            int vmId = cl.getVmId();
            int containerId = cl.getContainerId();

            // time to transfer the files
            double fileTransferTime = predictFileTransferTime(cl.getRequiredFiles());
            ContainerHost host=null;
            if(cl.getCloudletId()==1184){
                System.out.println(CloudSim.clock());
                host = getVmAllocationPolicy().getHost(vmId, userId);
            }
            else {
                host = getVmAllocationPolicy().getHost(vmId, userId);
            }

            ServerlessInvoker vm = null;
            Container container = null;
            double estimatedFinishTime = 0;

            vm = (ServerlessInvoker)host.getContainerVm(vmId, userId);
            container = vm.getContainer(containerId, userId);

            estimatedFinishTime =((ServerlessCloudletScheduler) container.getContainerCloudletScheduler()).cloudletSubmit(cl, vm, (ServerlessContainer)(container));
            System.out.println("Est finish time of function# "+ cl.getCloudletId()+" at the beginning is "+ (CloudSim.clock()+estimatedFinishTime));
            // }
//            count++;

            /** Update Vm Bin */
            updateOnlineVmBin((ServerlessInvoker)vm, cl.getArrivalTime()+cl.getMaxExecTime()-CloudSim.clock());
            updateRunTimeVm((ServerlessInvoker)vm, cl.getArrivalTime()+cl.getMaxExecTime()-CloudSim.clock());

            /** Send an event when 90% of the deadline is reached for a cloudlet */
//            System.out.println(CloudSim.clock()+" Deadline checkpoint time for cloudlet# "+cl.getCloudletId()+" is "+(CloudSim.clock()+(cl.getArrivalTime()+cl.getMaxExecTime()-CloudSim.clock())* Constants.DEADLINE_CHECKPOINT));
            if(cl.getPriority()==1){
                send(getId(), ((cl.getArrivalTime()+cl.getMaxExecTime()-CloudSim.clock())* Constants.DEADLINE_CHECKPOINT_LOW), CloudSimTags.DEADLINE_CHECKPOINT,cl);
            }
            else{
                send(getId(), ((cl.getArrivalTime()+cl.getMaxExecTime()-CloudSim.clock())* Constants.DEADLINE_CHECKPOINT_HIGH), CloudSimTags.DEADLINE_CHECKPOINT,cl);
            }

            /**Remove the new cloudlet's container from removal list*/
            getContainersToDestroy().remove(container);
            //System.out.println("Submit: Removed from destroy list container "+container.getId());
            // if this cloudlet is in the exec queue
            if (estimatedFinishTime > 0.0 && !Double.isInfinite(estimatedFinishTime)) {
                estimatedFinishTime += fileTransferTime;

                send(getId(), (estimatedFinishTime-CloudSim.clock()), CloudSimTags.VM_DATACENTER_EVENT);

                /*** PREEMPTION  ***/
                if (ack) {
                    System.out.println("Cloudlet" + cl.getCloudletId() +" in container "+ cl.getContainerId()+" is to be preempted at time "+(cl.getMaxExecTime() + cl.getArrivalTime()));
                    send(getId(), (cl.getMaxExecTime() + cl.getArrivalTime() - CloudSim.clock()+2), CloudSimTags.PREEMPT_CLOUDLET, cl);
                }
            }

            int tag = CloudSimTags.CLOUDLET_SUBMIT_ACK;
            sendNow(cl.getUserId(), tag, cl);

        } catch (ClassCastException c) {
            Log.printLine(String.format("%s.processCloudletSubmit(): ClassCastException error.", getName()));
            c.printStackTrace();
        } catch (Exception e) {
            Log.printLine(String.format("%s.processCloudletSubmit(): Exception error.", getName()));
            e.printStackTrace();
        }

        checkCloudletCompletion();
        if (Constants.containerConcurrency){
            containerHorizontalAutoScaling();
        }
        destroyIdleContainers();

        /**Update CPU Utilization of Vm */
        /*for(ContainerVm vm: getContainerVmList()){
            addToCPUUtilizationLog(vm.getId(),(vm.getTotalMips()-vm.getAvailableMips())/vm.getTotalMips());
        }*/



    }

    protected void containerHorizontalAutoScaling(){
        int userId = 0;
        Map<String, Map<String, Double>> fnNestedMap = new HashMap<>();
        Map<String, ArrayList<ServerlessContainer>> emptyContainers = new HashMap<>();
        List<? extends ContainerHost> list = getVmAllocationPolicy().getContainerHostList();
        for (ContainerHost host : list) {
            for (ContainerVm machine : host.getVmList()) {
                userId = machine.getUserId();
                ServerlessInvoker vm = (ServerlessInvoker)machine;
                for (Map.Entry<String, ArrayList<Container>> contMap : vm.getFunctionContainerMap().entrySet()) {
                    if(fnNestedMap.containsKey(contMap.getKey())){
                        fnNestedMap.get(contMap.getKey()).put("container_count", fnNestedMap.get(contMap.getKey()).get("container_count") + contMap.getValue().size());
                    }
                    else{
                        Map<String, Double> fnMap = new HashMap<>();
                        fnMap.put("container_count", (double) contMap.getValue().size());
                        fnMap.put("container_cpu_util", 0.0);
                        fnNestedMap.put(contMap.getKey(), fnMap);
                    }
                    for (Container cont : contMap.getValue()) {
                        ServerlessContainer container = (ServerlessContainer) cont;
                        if (container.getRunningTasks().size() == 0){
                            if (!emptyContainers.containsKey(contMap.getKey())) {
                                emptyContainers.put(contMap.getKey(), new ArrayList<>());
                            }
                            emptyContainers.get(contMap.getKey()).add(container);
                        }
                        fnNestedMap.get(contMap.getKey()).put("container_cpu_util", Double.sum(fnNestedMap.get(contMap.getKey()).get("container_cpu_util"), (((ServerlessCloudletScheduler)(container.getContainerCloudletScheduler())).getTotalCurrentAllocatedMipsShareForCloudlets()).get(0)));
                    }
                }
                for (Map.Entry<String, ArrayList<Container>> contMap : vm.getFunctionContainerMapPending().entrySet()) {
                    if(fnNestedMap.containsKey(contMap.getKey())){
                        fnNestedMap.get(contMap.getKey()).put("pending_container_count", fnNestedMap.get(contMap.getKey()).get("pending_container_count") + contMap.getValue().size());
                    }
                    else{
                        Map<String, Double> fnMap = new HashMap<>();
                        fnMap.put("pending_container_count", (double) contMap.getValue().size());
                        fnNestedMap.put(contMap.getKey(), fnMap);
                    }
                }

            }
        }
        for (Map.Entry<String, Map<String, Double>> data : fnNestedMap.entrySet()) {
            int desiredReplicas = (int) Math.ceil(data.getValue().get("container_cpu_util")/data.getValue().get("container_count")/Constants.containerScaleCPUThreshold);
            int newReplicaCount;
            int newReplicasToCreate;
            int replicasToRemove;
            if (desiredReplicas > 0){
                newReplicaCount = Math.min(desiredReplicas, Constants.maxReplicas);
            }
            else{
                newReplicaCount = 1;
            }
            if (newReplicaCount > data.getValue().get("container_count") + data.getValue().get("pending_container_count")){
                newReplicasToCreate = (int) Math.ceil(newReplicaCount - data.getValue().get("container_count") - data.getValue().get("pending_container_count"));
                for (int x = 0; x < newReplicasToCreate; x++){
                    int[] dt = new int[2];
                    dt[0] = userId;
                    dt[1] = Integer.parseInt(data.getKey());

                    sendNow(userId, CloudSimTags.SCALED_CONTAINER, dt);
                }
            }
            if (newReplicaCount < data.getValue().get("container_count") + data.getValue().get("pending_container_count")){
                replicasToRemove = (int) Math.ceil(data.getValue().get("container_count") + data.getValue().get("pending_container_count") - newReplicaCount);
                int removedContainers = 0;
                for (ServerlessContainer cont : emptyContainers.get(data.getKey())) {
                    getContainersToDestroy().add(cont);
                    removedContainers ++;
                    if (removedContainers == replicasToRemove){
                        break;
                    }
                }
            }

        }
    }

    protected Map<String,List<Integer>> containerVerticalAutoScaling(String functionId){
        Map<String,List<Integer>> unAvailableActionMap =new HashMap<>();
        double peMIPSForContainerType = 0;
        double ramForContainerType = 0;
        ArrayList<Integer> unAVailableActionlistCPU = new ArrayList<Integer>();
        ArrayList<Integer> unAVailableActionlistRam = new ArrayList<Integer>();
        List<? extends ContainerHost> list = getVmAllocationPolicy().getContainerHostList();
        for (ContainerHost host : list) {
            for (ContainerVm machine : host.getVmList()) {
                double containerCPUUtilMin = 0;
                double containerRAMUtilMin = 0;
                ServerlessInvoker vm = (ServerlessInvoker) machine;
                double vmUsedupRam = vm.getContainerRamProvisioner().getRam() - vm.getContainerRamProvisioner().getAvailableVmRam();
                double vmUsedupMIPS = vm.getContainerScheduler().getPeCapacity()*vm.getContainerScheduler().getPeList().size() - vm.getContainerScheduler().getAvailableMips();
                int numContainers = vm.getFunctionContainerMap().get(functionId).size();
                for (Container cont : vm.getFunctionContainerMap().get(functionId)) {
                    peMIPSForContainerType = cont.getMips();
                    ramForContainerType = cont.getRam();
                    ServerlessContainer container = (ServerlessContainer)cont;
                    ServerlessCloudletScheduler clScheduler = (ServerlessCloudletScheduler) (container.getContainerCloudletScheduler());
                    if (clScheduler.getTotalCurrentAllocatedMipsShareForCloudlets().get(0) > containerCPUUtilMin){
                        containerCPUUtilMin = clScheduler.getTotalCurrentAllocatedMipsShareForCloudlets().get(0);
                    }
                    if (clScheduler.getTotalCurrentAllocatedRamForCloudlets() > containerRAMUtilMin){
                        containerRAMUtilMin = clScheduler.getTotalCurrentAllocatedRamForCloudlets();
                    }

                }
                for (int x = 0; x< (Constants.CONTAINER_RAM_INCREMENT).length; x++){
                    if (!unAVailableActionlistRam.contains(x)){
                        if (Constants.CONTAINER_RAM_INCREMENT[x]*numContainers > vm.getContainerRamProvisioner().getAvailableVmRam() || Constants.CONTAINER_RAM_INCREMENT[x]*numContainers + vmUsedupRam < 0 || (ramForContainerType + Constants.CONTAINER_RAM_INCREMENT[x]) > Constants.MAX_CONTAINER_RAM || (ramForContainerType + Constants.CONTAINER_RAM_INCREMENT[x]) < Constants.MIN_CONTAINER_RAM || (ramForContainerType + Constants.CONTAINER_RAM_INCREMENT[x]) < containerRAMUtilMin){
                            unAVailableActionlistRam.add(x);
                        }
                    }
                }
                for (int x = 0; x< (Constants.CONTAINER_MIPS_INCREMENT).length; x++){
                    if (!unAVailableActionlistCPU.contains(x)){
                        if (Constants.CONTAINER_MIPS_INCREMENT[x]*numContainers*Constants.CONTAINER_PES[Integer.parseInt(functionId)] > vm.getContainerScheduler().getAvailableMips() || Constants.CONTAINER_MIPS_INCREMENT[x]*numContainers*Constants.CONTAINER_PES[Integer.parseInt(functionId)] + vmUsedupMIPS < 0 || (peMIPSForContainerType + Constants.CONTAINER_MIPS_INCREMENT[x]) > Constants.MAX_CONTAINER_MIPS || (peMIPSForContainerType + Constants.CONTAINER_MIPS_INCREMENT[x]) < Constants.MIN_CONTAINER_MIPS || (peMIPSForContainerType + Constants.CONTAINER_MIPS_INCREMENT[x]) < containerCPUUtilMin){
                            unAVailableActionlistCPU.add(x);
                        }
                    }
                }

            }
        }
        unAvailableActionMap.put("cpuActions", unAVailableActionlistCPU);
        unAvailableActionMap.put("memActions", unAVailableActionlistRam);
        return unAvailableActionMap;

    }

    @Override

    protected void checkCloudletCompletion() {
        List<? extends ContainerHost> list = getVmAllocationPolicy().getContainerHostList();
        for (int i = 0; i < list.size(); i++) {
            ContainerHost host = list.get(i);
            for (ContainerVm vm : host.getVmList()) {
                for (Container container : vm.getContainerList()) {
                    while (container.getContainerCloudletScheduler().isFinishedCloudlets()) {
                        Cloudlet cl = container.getContainerCloudletScheduler().getNextFinishedCloudlet();
                        if (cl != null) {
                            Pair data = new Pair<>(cl, vm);
                            for(int x=0; x<((ServerlessInvoker)vm).getRunningCloudletList().size();x++){
                                if(((ServerlessInvoker)vm).getRunningCloudletList().get(x)==(ServerlessTasks)cl){
                                    ((ServerlessInvoker)vm).getRunningCloudletList().remove(x);
                                }
                            }

                            sendNow(cl.getUserId(), CloudSimTags.CLOUDLET_RETURN, data);
//                            removeFromVmTaskMap((ServerlessTasks)cl,(ServerlessInvoker)vm);
                        }
                    }
                }
            }
        }
    }

    /** Destroy idling containers*/
    protected void destroyIdleContainers() {
        while (!getContainersToDestroy().isEmpty()) {
            for (int x = 0; x < getContainersToDestroy().size(); x++) {
                if (((ServerlessContainer) getContainersToDestroy().get(x)).newContainer) {
//                    System.out.println("Container to be destroyed removed since new: "+e.getContainersToDestroy().get(x).getId());
                    containersToDestroy.remove(x);
                    continue;
                }
//                System.out.println(((ServerlessContainer)e.getContainersToDestroy().get(x)).newContainer);
//                    System.out.println("Container to be destroyed: " + e.getContainersToDestroy().get(x).getId());
                sendNow(getId(), CloudSimTags.CONTAINER_DESTROY_ACK, getContainersToDestroy().get(x));
            }
            containersToDestroy.clear();
        }

    }

    public void updateRunTimeVm(ServerlessInvoker vm, double time){
        if(runTimeVm.get(vm.getId())<time){
            runTimeVm.put(vm.getId(),time);
        }
    }

    /**Creates online bin        */
    public void createOnlineVmBin(){
//        System.out.println("PRINTING RUNTIMEVM: "+runTimeVm);
        onlineBinOfVms.clear();
        binNos.clear();
        for (Map.Entry<Integer, Double> entry : runTimeVm.entrySet()) {

            if(entry.getValue()>1) {
//                System.out.println(">>>>>*******************Debug: Bin added ");
                int bin = (int) (Math.ceil(Math.log(entry.getValue()) / Math.log(2)));

                if(!onlineBinOfVms.containsKey(bin)) {
                    ArrayList<Integer> instanceIds = new ArrayList<>();
                    instanceIds.add(entry.getKey());
                    onlineBinOfVms.put(bin, instanceIds);
                }
                else{
                    onlineBinOfVms.get(bin).add(entry.getKey());
                }
                if(!binNos.contains(bin)) {
                    binNos.add(bin);
                }
//                System.out.println(CloudSim.clock()+" Bin "+bin+" is added to binArray");
                Collections.sort(binNos);
            }
            else if(entry.getValue()>0 && entry.getValue()<1){
                int bin = 1;
                if(!onlineBinOfVms.containsKey(bin)) {
                    ArrayList<Integer> instanceIds = new ArrayList<>();
                    instanceIds.add(entry.getKey());
                    onlineBinOfVms.put(bin, instanceIds);
                }
                else{
                    onlineBinOfVms.get(bin).add(entry.getKey());
                }
                if(!binNos.contains(bin)) {
                    binNos.add(bin);
                }

//                System.out.println(CloudSim.clock()+" Bin "+bin+" is added to binArray");
                Collections.sort(binNos);
            }
            else if(entry.getValue()==0 ){
                int bin = 0;
                if(!onlineBinOfVms.containsKey(bin)) {
                    ArrayList<Integer> instanceIds = new ArrayList<>();
                    instanceIds.add(entry.getKey());
                    onlineBinOfVms.put(bin, instanceIds);
                }
                else{
                    onlineBinOfVms.get(bin).add(entry.getKey());
                }
                if(!binNos.contains(bin)) {
                    binNos.add(bin);
                }

//                System.out.println(CloudSim.clock()+" Bin "+bin+" is added to binArray");
                Collections.sort(binNos);
            }
        }

    }

    /**Updates online bin        */
    public void updateOnlineVmBin(ServerlessInvoker vm, double time){
        if(runTimeVm.get(vm.getId())<time){
            if(runTimeVm.get(vm.getId())>1) {
                int oldBin = (int) (Math.ceil(Math.log(runTimeVm.get(vm.getId())) / Math.log(2)));
//                System.out.println(("Debug: Old bin: "+ oldBin));

                if(onlineBinOfVms.containsKey(oldBin)) {

                    onlineBinOfVms.get(oldBin).remove(new Integer(vm.getId()));
                    if ((onlineBinOfVms.get(oldBin)).size() ==0) {
                        onlineBinOfVms.remove(oldBin);

                        for(int x=0; x<binNos.size(); x++){
                            if(binNos.get(x)==oldBin){
                                binNos.remove(x);
                                break;
                            }

                        }

                    }
                }



//                System.out.println("Update bin time "+time);
                int newBin = (int) (Math.ceil(Math.log(time) / Math.log(2)));
//                System.out.println("Debug: New bin: " + newBin);
                if(onlineBinOfVms.containsKey(newBin)){
                    onlineBinOfVms.get(newBin).add(vm.getId());
                }
                else{
//                    System.out.println("New bin added: New bin "+newBin+" "+"with vm "+vm.getId());
                    ArrayList<Integer> instanceIds = new ArrayList<>();
                    instanceIds.add(vm.getId());
                    onlineBinOfVms.put(newBin,instanceIds);
                    if(!binNos.contains(newBin)) {
                        binNos.add(newBin);
                    }
//                    System.out.println(CloudSim.clock()+" Bin "+newBin+" is added to binArray");
                    Collections.sort(binNos);

                }

            }
            else if(runTimeVm.get(vm.getId())>0 && runTimeVm.get(vm.getId())<1){
                int oldBin = 1;
//                System.out.println(("Debug: Old bin: "+ oldBin));
                if(onlineBinOfVms.containsKey(oldBin)) {

                    onlineBinOfVms.get(oldBin).remove(new Integer(vm.getId()));
                    if ((onlineBinOfVms.get(oldBin)).size() ==0) {
                        onlineBinOfVms.remove(oldBin);
                        for(int x=0; x<binNos.size(); x++){
                            if(binNos.get(x)==oldBin){
                                binNos.remove(x);
                                break;
                            }

                        }
                    }
                }

//                System.out.println("Update bin time "+time);
                if(time>1){
                    int newBin = (int) (Math.ceil(Math.log(time) / Math.log(2)));
//                    System.out.println("Debug: New bin: " + newBin);
                    if(onlineBinOfVms.containsKey(newBin)){
                        onlineBinOfVms.get(newBin).add(vm.getId());
                    }
                    else{
//                        System.out.println("New bin added: New bin "+newBin+" "+"with vm "+vm.getId());
                        ArrayList<Integer> instanceIds = new ArrayList<>();
                        instanceIds.add(vm.getId());
                        onlineBinOfVms.put(newBin,instanceIds);
                        if(!binNos.contains(newBin)) {
                            binNos.add(newBin);
                        }
//                        System.out.println(CloudSim.clock()+" Bin "+newBin+" is added to binArray");
                        Collections.sort(binNos);

                    }
                }
                else{
                    int newBin = 1;
//                    System.out.println("Debug: New bin: " + newBin);
                    if(onlineBinOfVms.containsKey(newBin)){
                        onlineBinOfVms.get(newBin).add(vm.getId());
                    }
                    else{
//                        System.out.println("New bin added: New bin "+newBin+" "+"with vm "+vm.getId());
                        ArrayList<Integer> instanceIds = new ArrayList<>();
                        instanceIds.add(vm.getId());
                        onlineBinOfVms.put(newBin,instanceIds);
                        if(!binNos.contains(newBin)) {
                            binNos.add(newBin);
                        }
//                        System.out.println(CloudSim.clock()+" Bin "+newBin+" is added to binArray");
                        Collections.sort(binNos);

                    }

                }


            }
            else if(runTimeVm.get(vm.getId())==0 ){
                int oldBin = 0;
//                System.out.println(("Debug: Old bin: "+ oldBin));
                if(onlineBinOfVms.containsKey(oldBin)) {

                    onlineBinOfVms.get(oldBin).remove(new Integer(vm.getId()));
                    if ((onlineBinOfVms.get(oldBin)).size() ==0) {
                        onlineBinOfVms.remove(oldBin);
                        for(int x=0; x<binNos.size(); x++){
                            if(binNos.get(x)==oldBin){
                                binNos.remove(x);
                                break;
                            }

                        }
                    }
                }

//                System.out.println("Update bin time "+time);
                if(time>1){
                    int newBin = (int) (Math.ceil(Math.log(time) / Math.log(2)));
//                    System.out.println("Debug: New bin: " + newBin);
                    if(onlineBinOfVms.containsKey(newBin)){
                        onlineBinOfVms.get(newBin).add(vm.getId());
                    }
                    else{
//                        System.out.println("New bin added: New bin "+newBin+" "+"with vm "+vm.getId());
                        ArrayList<Integer> instanceIds = new ArrayList<>();
                        instanceIds.add(vm.getId());
                        onlineBinOfVms.put(newBin,instanceIds);
                        if(!binNos.contains(newBin)) {
                            binNos.add(newBin);
                        }
//                        System.out.println(CloudSim.clock()+" Bin "+newBin+" is added to binArray");
                        Collections.sort(binNos);

                    }
                }
                else{
                    int newBin = 1;
//                    System.out.println("Debug: New bin: " + newBin);
                    if(onlineBinOfVms.containsKey(newBin)){
                        onlineBinOfVms.get(newBin).add(vm.getId());
                    }
                    else{
//                        System.out.println("New bin added: New bin "+newBin+" "+"with vm "+vm.getId());
                        ArrayList<Integer> instanceIds = new ArrayList<>();
                        instanceIds.add(vm.getId());
                        onlineBinOfVms.put(newBin,instanceIds);
                        if(!binNos.contains(newBin)) {
                            binNos.add(newBin);
                        }
//                        System.out.println(CloudSim.clock()+" Bin "+newBin+" is added to binArray");
                        Collections.sort(binNos);

                    }

                }


            }



        }

    }

}


