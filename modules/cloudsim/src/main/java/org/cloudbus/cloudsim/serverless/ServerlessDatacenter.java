package org.cloudbus.cloudsim.serverless;

import javafx.util.Pair;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.container.core.*;
import org.cloudbus.cloudsim.container.resourceAllocators.ContainerVmAllocationPolicy;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;

import java.text.DecimalFormat;
import java.util.*;

public class ServerlessDatacenter extends PowerContainerDatacenterCM {
    private static final DecimalFormat df = new DecimalFormat("0.00");
    /**
     * requests to be rescheduled
     */
    private final Map<Integer, ServerlessRequest> tasksWaitingToReschedule;

    /**
     * Idle Vm list
     */
    private static List<ServerlessInvoker> vmIdleList = new ArrayList<>();
    /**
     * request reschedule event
     */
    private boolean reschedule = false;
    /**
     * The longest reaming run time of the requests running on each vm
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
     * request submit event of DC event
     */
    protected boolean autoScalingInitialized = false;

    /**
     * The load balancerfor DC.
     */
    private RequestLoadBalancer requestLoadBalancer;
    private FunctionAutoScaler autoScaler;

    private FunctionScheduler fnsched;


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


    public ServerlessDatacenter(String name, ContainerDatacenterCharacteristics characteristics, ContainerVmAllocationPolicy vmAllocationPolicy, FunctionScheduler containerAllocationPolicy, List<Storage> storageList, double schedulingInterval, String experimentName, String logAddress, double vmStartupDelay, double containerStartupDelay, boolean monitor) throws Exception {
        super(name, characteristics, vmAllocationPolicy, containerAllocationPolicy, storageList, schedulingInterval, experimentName, logAddress, vmStartupDelay, containerStartupDelay);
        tasksWaitingToReschedule = new HashMap<Integer, ServerlessRequest>();
        setMonitoring(monitor);
//        setContainerAllocationPolicy(containerAllocationPolicy);
        autoScaler = new FunctionAutoScaler(this);


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

//    public FunctionScheduler getContainerAllocationPolicy() {
//        return fnsched;
//    }

    public void setMonitoring(boolean monitor) {
        this.monitoring = monitor;
    }
    public void setRequestLoadBalancerR(RequestLoadBalancer lb) {
        this.requestLoadBalancer = lb;
    }

//    public void setContainerAllocationPolicy(FunctionScheduler scheduler) {
//        fnsched = scheduler;
//    }


    @Override
    protected void processOtherEvent(SimEvent ev) {
        switch (ev.getTag()) {
//            case CloudSimSCTags.DEADLINE_CHECKPOINT:
//                processDeadlineCheckpoint(ev, false);
//                break;
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

            case CloudSimSCTags.AUTO_SCALE:
                processAutoScaling(ev);
                break;

//            case CloudSimSCTags.PREEMPT_REQUEST:
//                preemptRequest(ev);
//                break;


            // other unknown tags are processed by this method
            default:
                super.processOtherEvent(ev);
                break;
        }
    }

//    public void preemptRequest(SimEvent ev){
//        ServerlessRequest cl = (ServerlessRequest) ev.getData();
//        ContainerHost host = getVmAllocationPolicy().getHost(cl.getVmId(),cl.getUserId() );
//        ServerlessInvoker vm = (ServerlessInvoker) host.getContainerVm(cl.getVmId(),cl.getUserId());
//        Container container = vm.getContainer(cl.getContainerId(), cl.getUserId());
//        if (container != null) {
//            double remainingLength = cl.getCloudletLength()-cl.getCloudletFinishedSoFar();
//            if (remainingLength > 10) {
//
//                System.out.println("request" + cl.getCloudletId() +" in container "+ cl.getContainerId()+" is preempted");
//                Cloudlet returnTask = getVmAllocationPolicy().getHost(cl.getVmId(), cl.getUserId()).getContainerVm(cl.getVmId(), cl.getUserId()).getContainer(((ServerlessRequest) cl).getContainerId(), cl.getUserId())
//                        .getContainerCloudletScheduler().cloudletCancel(cl.getCloudletId());
//
//                sendNow(this.getId(), CloudSimTags.CONTAINER_DESTROY_ACK,container);
//            }
//        }
//
//    }

    public void containerVerticalScale(Container container, ServerlessInvoker vm, int cpuChange, int memChange){
        boolean result = ((FunctionScheduler) getContainerAllocationPolicy()).reallocateVmResourcesForContainer(container, vm, cpuChange, memChange);

    }

    /** Process event for deadline checkpointing */
//    public void processDeadlineCheckpoint(SimEvent ev, boolean ack){
//        /*if(CloudSim.clock()==11.251999999999999){
//            System.out.println("Debug");
//        }*/
//
//        updateCloudletProcessing();
//        ServerlessRequest cl = (ServerlessRequest) ev.getData();
//        if(cl.getStatus()==3) {
//            ContainerHost host = getVmAllocationPolicy().getHost(cl.getVmId(),cl.getUserId() );
//            ServerlessInvoker vm = (ServerlessInvoker) host.getContainerVm(cl.getVmId(),cl.getUserId());
//            Container container = vm.getContainer(cl.getContainerId(), cl.getUserId());
//            // System.out.println(CloudSim.clock()+" Debug:DC: request's container is "+ container);
//
////        double timeSpan = CloudSim.clock() - (container.getContainerrequestScheduler()).getPreviousTime();
//
////        double remainingLength = cl.getrequestLength()-(cl.getrequestFinishedSoFar()+ (((ServerlessrequestScheduler) (container.getContainerrequestScheduler())).getTotalMips())*timeSpan);
//
//            double remainingLength = cl.getCloudletLength()-cl.getCloudletFinishedSoFar();
//
//
//            if (remainingLength > 1) {
//                System.out.println(CloudSim.clock()+" Debug:DC: request #"+cl.getCloudletId()+" has not finished > Reschedule");
//
////            if(vm.getTotalMips())
//                double vmCPUUsageBefore=1 - vm.getAvailableMips() / vm.getTotalMips();
//                //System.out.println(CloudSim.clock()+" vmCPUUsageBefore: "+vmCPUUsageBefore);
//                if(vmCPUUsageBefore <= Constants.VM_CPU_USAGE_THRESHOLD) {
//                    boolean result = ((FunctionScheduler) getContainerAllocationPolicy()).reallocateVmResourcesForContainer(container, vm,cl);
//                    if (result) {
////                    updaterequestProcessing();
////                    System.out.println(container.getMips());
//                        double estimatedFinishTime = remainingLength / (container.getMips());
//                        double delay = 0;
//                        if(cl.getPriority()==1){
//                            delay = (cl.getArrivalTime() + cl.getMaxExecTime() - CloudSim.clock()) * Constants.DEADLINE_CHECKPOINT_LOW;
//                        }
//                        else{
//                            delay = (cl.getArrivalTime() + cl.getMaxExecTime() - CloudSim.clock()) * Constants.DEADLINE_CHECKPOINT_HIGH;
//                        }
//                        if (delay > 1) {
//                            send(getId(), delay, CloudSimSCTags.DEADLINE_CHECKPOINT, cl);
//                        }
////                        send(getId(), ((cl.getArrivalTime() + cl.getMaxExecTime() - CloudSim.clock()) * Constants.DEADLINE_CHECKPOINT), CloudSimTags.DEADLINE_CHECKPOINT, cl);
//                        send(getId(), estimatedFinishTime, CloudSimTags.VM_DATACENTER_EVENT);
//
//                        System.out.println(CloudSim.clock() + " Debug:DC: Container #" + container.getId() + " now has: " + container.getRam() + " ram and " + container.getMips() + " MIPS");
//                        /** Check now if the node is constrained */
//                        double vmCPUUsageAfter = 1 - vm.getAvailableMips() / vm.getTotalMips();
//                        System.out.println(CloudSim.clock() + " Debug:DC: vm #" + vm.getId() +" vmCPUUsageAfter: "+vmCPUUsageAfter);
//
//
//
//
//
//                        ArrayList<ResCloudlet> toRemove = new ArrayList<ResCloudlet>();
//                        double mipsFreed = 0;
//                        //System.out.println("request Stack!: " + vm.getRunningrequestStack());
//                        for (ServerlessRequest task : vm.getRunningrequestList()) {
//                            System.out.println(task.getCloudletId() +" "+(task.getArrivalTime()+task.getMaxExecTime())+ ", ");
//                        }
//                        if(vm.getRunningrequestList().size()>0) {
//                            while (vmCPUUsageAfter > Constants.VM_CPU_USAGE_THRESHOLD) {
//
//                                for (int i = vm.getRunningrequestList().size() - 1; i >= 0; i--) {
//
//                                    ServerlessRequest toReschedule = vm.getRunningrequestList().remove(i);
//                                    //System.out.println("request: " + toReschedule.getrequestId() + " SPent time: " + (CloudSim.clock() - toReschedule.getArrivalTime()) + " Available time: " + toReschedule.getMaxExecTime() * Constants.DEADLINE_CHECKPOINT);
//                                    ServerlessContainer requestCont = ContainerList.getById(getContainerList(), toReschedule.getContainerId());
//                                    if(toReschedule.getCloudletId()==1253 && toReschedule.reschedule){
//                                        System.out.println("my reschedule status "+ toReschedule.reschedule);
//                                    }
//                                    if(requestCont!=null) {
//                                        if ((CloudSim.clock() - toReschedule.getArrivalTime() + Constants.FUNCTION_SCHEDULING_DELAY) < toReschedule.getMaxExecTime() * 0.2 && requestCont.getContainerCloudletScheduler().getCloudletWaitingList().isEmpty() && toReschedule.reschedule!=true && toReschedule.getPriority()==2) {
//
//                                            System.out.println(CloudSim.clock() + " Debug:DC: Evict request #" + toReschedule.getCloudletId() + " in container " + requestCont.getId());
//                                            //System.out.println(CloudSim.clock() + " Debug:DC: Container #" + requestCont.getId() + " execution list is " + requestCont.getContainerrequestScheduler().getrequestExecList() + " and waiting list is " + requestCont.getContainerrequestScheduler().getrequestWaitingList());
//                                            while (!requestCont.getContainerCloudletScheduler().getCloudletExecList().isEmpty() || !requestCont.getContainerCloudletScheduler().getCloudletWaitingList().isEmpty()) {
//                                                for (ResCloudlet rcl : requestCont.getContainerCloudletScheduler().getCloudletExecList()) {
//                                                    rescheduleRequest((ServerlessRequest) rcl.getCloudlet(),requestCont);
//                                                    toReschedule.setReschedule(true);
//                                                    System.out.println("my reschedule status after"+ toReschedule.reschedule);
//                                                    toRemove.add(rcl);
//                                                }
//
//                                                for (ResCloudlet rcl : requestCont.getContainerCloudletScheduler().getCloudletWaitingList()) {
//                                                    rescheduleRequest((ServerlessRequest) rcl.getCloudlet(), requestCont);
//                                                    toReschedule.setReschedule(true);
//                                                    System.out.println("my reschedule status after"+ toReschedule.reschedule);
//                                                    toRemove.add(rcl);
//                                                }
//
//                                                for (int x = 0; x < toRemove.size(); x++) {
//                                                    Cloudlet remove = toRemove.get(x).getCloudlet();
//                                                    Cloudlet returnTask = getVmAllocationPolicy().getHost(remove.getVmId(), remove.getUserId()).getContainerVm(remove.getVmId(), remove.getUserId()).getContainer(((ServerlessRequest) remove).getContainerId(), remove.getUserId())
//                                                            .getContainerCloudletScheduler().cloudletCancel(remove.getCloudletId());
//                                                }
////                                requestCont.getContainerrequestScheduler().getrequestExecList().removeAll(toRemove);
//
////                                requestCont.getContainerrequestScheduler().getrequestWaitingList().removeAll(toRemove);
//                                                toRemove.clear();
//                                            }
//
//                                            mipsFreed += requestCont.getMips();
//                                            vmCPUUsageAfter = 1 - (vm.getAvailableMips() + mipsFreed) / vm.getTotalMips();
//                                            System.out.println(CloudSim.clock() + " Debug:DC: CPU usage is now " + vmCPUUsageAfter);
//
//                                            //*** add request's old container to the removal list
//                                            getContainersToDestroy().add(requestCont);
//                                            //System.out.println("Container to be destroyed due to contention: " + requestCont.getId());
//                                            System.out.println(CloudSim.clock() + " Debug:Due to rescheduling destroy container " + requestCont.getId());
//                                            if (vmCPUUsageAfter < Constants.VM_CPU_USAGE_THRESHOLD)
//                                                break;
//                                        }
//                                    }
//                                }
//                                break;
//
//                            }
//                        }
//
//
//
//
//
//                    }
//                    else {
//                        System.out.println(CloudSim.clock() + " Debug:DC: Rescheduling for container #" + container.getId() + " failed");
//                        double delay=0;
//                        if(cl.getPriority()==1){
//                            delay = ((cl.getArrivalTime() + cl.getMaxExecTime() - CloudSim.clock()) * Constants.DEADLINE_CHECKPOINT_LOW);
//                        }
//                        else{
//                            delay = ((cl.getArrivalTime() + cl.getMaxExecTime() - CloudSim.clock()) * Constants.DEADLINE_CHECKPOINT_HIGH);
//                        }
////                         delay = ((cl.getArrivalTime() + cl.getMaxExecTime() - CloudSim.clock()) * Constants.DEADLINE_CHECKPOINT);
//                        if (delay > 1) {
//                            send(getId(), delay, CloudSimSCTags.DEADLINE_CHECKPOINT, cl);
//                        }
//                    }
//                }
//                else{
//                    double delay=0;
//                    if(cl.getPriority()==1){
//                        delay = ((cl.getArrivalTime() + cl.getMaxExecTime() - CloudSim.clock()) * Constants.DEADLINE_CHECKPOINT_LOW);
//                    }
//                    else{
//                        delay = ((cl.getArrivalTime() + cl.getMaxExecTime() - CloudSim.clock()) * Constants.DEADLINE_CHECKPOINT_HIGH);
//                    }
////                    delay = ((cl.getArrivalTime() + cl.getMaxExecTime() - CloudSim.clock()) * Constants.DEADLINE_CHECKPOINT);
//                    if(delay>1) {
//                        send(getId(), delay, CloudSimSCTags.DEADLINE_CHECKPOINT, cl);
//                    }
//
//                }
//            }
//
//        }
//
//        /** Destroy idling containers*/
//        while(!containersToDestroy.isEmpty()){
//            for(int x=0; x<containersToDestroy.size(); x++){
//                if(((ServerlessContainer) getContainersToDestroy().get(x)).newContainer){
////                    System.out.println("Container to be destroyed removed since new: "+getContainersToDestroy().get(x).getId());
//                    getContainersToDestroy().remove(x);
//                    continue;
//                }
//                //System.out.println("Container to be destroyed: "+containersToDestroy.get(x).getId());
//                if(containersToDestroy.get(x).getId()==94){
//                    System.out.println("to be destroyed 94 at checkpoint");
//                }
//                sendNow(this.getId(), CloudSimTags.CONTAINER_DESTROY_ACK,containersToDestroy.get(x));
//            }
//            containersToDestroy.clear();
//        }
//
//
//    }

//    public void rescheduleRequest(ServerlessRequest request, ServerlessContainer container) {
//        System.out.println("Debug DC: Trying to reschedule request #" + request.getCloudletId());
//
//        /*** Updating task memory to current ***/
//        request.setRequestMemory((int) container.getCurrentAllocatedRam());
//        tasksWaitingToReschedule.put(request.getCloudletId(), request);
//
//        send(request.getUserId(), Constants.FUNCTION_SCHEDULING_DELAY, CloudSimSCTags.CLOUDLET_RESCHEDULE, request);
//
//    }

//    @Override
//    protected void processCloudletMove(int[] receivedData, int type) {
//        updateCloudletProcessing();
//
//        int[] array = receivedData;
//        int requestId = array[0];
//        int userId = array[1];
//        int vmId = array[2];
//        int containerId = array[3];
//        int vmDestId = array[4];
//        int containerDestId = array[5];
//        int destId = array[6];
//        ServerlessInvoker newContainerVm = null;
//
//        // get the request
////        request cl = getVmAllocationPolicy().getHost(vmId, userId).getContainerVm(vmId, userId).getContainer(containerId, userId)
////                .getContainerrequestScheduler().requestCancel(requestId);
//
//        ServerlessRequest cl = tasksWaitingToReschedule.get(requestId);
//        tasksWaitingToReschedule.remove(requestId,cl);
//        ServerlessInvoker oldVm = (ServerlessInvoker) getVmAllocationPolicy().getHost(vmId, userId).getContainerVm(vmId, userId);
//
//        /** Remove request from old vmtaskmap */
////        removeFromVmTaskMap((ServerlessRequest)cl,oldVm);
//
//
//
//        boolean failed = false;
//        if (cl == null) {// request doesn't exist
//            failed = true;
//        } else {
//            // has the request already finished?
//            if (cl.getCloudletStatusString().equals("Success")) {// if yes, send it back to user
//                int[] data = new int[3];
//                data[0] = getId();
//                data[1] = requestId;
//                data[2] = 0;
//                sendNow(cl.getUserId(), CloudSimTags.CLOUDLET_SUBMIT_ACK, data);
//                sendNow(cl.getUserId(), CloudSimTags.CLOUDLET_RETURN, cl);
//            }
//
//            // prepare request for migration
//            cl.setVmId(vmDestId);
//            ((ContainerCloudlet)cl).setContainerId(containerDestId);
//
//
//
//            // the request will migrate from one vm to another does the destination VM exist?
//            if (destId == getId()) {
//                newContainerVm = (ServerlessInvoker) getVmAllocationPolicy().getHost(vmDestId, userId).getContainerVm(vmDestId, userId);
//
//                /** Add request to new vmtaskmap */
////                addToVmTaskMap((ServerlessRequest)cl,newContainerVm);
//
//                if (newContainerVm == null) {
//                    failed = true;
//                } else {
//
//                    // time to transfer the files
//                    double fileTransferTime = predictFileTransferTime(cl.getRequiredFiles());
//                    //System.out.println("Vm "+ newContainerVm.getId()+"Cont list size: "+newContainerVm.getContainerList().size());
//                    /*for(Container cont: newContainerVm.getContainerList()){
//                        System.out.println(cont.getId());
//                    }*/
//                    ServerlessContainer newContainer = (ServerlessContainer)(getVmAllocationPolicy().getHost(vmDestId, userId).getContainerVm(vmDestId, userId).getContainer(containerDestId, userId));
//
//                    cl.setResourceParameter(getId(), getCharacteristics().getCostPerSecond(), getCharacteristics()
//                            .getCostPerBw(), cl.getVmId());
//                    //System.out.println("request scheduler!!! cont ID "+newContainer);
//
//                    /*** set new MIPS to all containers ***/
//
////                    reprovisionMipsToAllContainers(newContainerVm);
//
//
//
//
//
//
//
//
//
//
//
//                    System.out.println(("request scheduler!!! cont ID "+newContainer.getId()+" "+(ServerlessRequestScheduler)newContainer.getContainerCloudletScheduler()));
//                    double estimatedFinishTime= ((ServerlessRequestScheduler)newContainer.getContainerCloudletScheduler()).requestSubmit(cl, newContainerVm, newContainer);
//
//                    /** Update vm bin with new request's deadline */
//                    updateOnlineVmBin(newContainerVm, ((ServerlessRequest)cl).getArrivalTime()+((ServerlessRequest)cl).getMaxExecTime()-CloudSim.clock());
//                    updateRunTimeVm(newContainerVm, ((ServerlessRequest)cl).getArrivalTime()+((ServerlessRequest)cl).getMaxExecTime()-CloudSim.clock());
//
//                    /** Send an event when 90% of the deadline is reached for a request */
//                    if(cl.getPriority()==1){
//                        send(getId(), ((((ServerlessRequest)cl).getArrivalTime()+((ServerlessRequest)cl).getMaxExecTime()-CloudSim.clock())* Constants.DEADLINE_CHECKPOINT_LOW), CloudSimSCTags.DEADLINE_CHECKPOINT,cl);
//                    }
//                    else{
//                        send(getId(), ((((ServerlessRequest)cl).getArrivalTime()+((ServerlessRequest)cl).getMaxExecTime()-CloudSim.clock())* Constants.DEADLINE_CHECKPOINT_HIGH), CloudSimSCTags.DEADLINE_CHECKPOINT,cl);
//                    }
////                    send(getId(), ((((ServerlessRequest)cl).getArrivalTime()+((ServerlessRequest)cl).getMaxExecTime()-CloudSim.clock())* Constants.DEADLINE_CHECKPOINT), CloudSimTags.DEADLINE_CHECKPOINT,cl);
//
//                    // if this request is in the exec queue
//                    if (estimatedFinishTime > 0.0 && !Double.isInfinite(estimatedFinishTime)) {
//                        estimatedFinishTime += fileTransferTime;
//
//                        /**Remove the new request's new container from removal list */
//                        getContainersToDestroy().remove(newContainer);
//                        //System.out.println("request move: Removed from destroy list container "+newContainer.getId());
////                        getContainersToDestroy().add((ServerlessContainer)newContainerVm.getContainer(containerId, userId));
//
//                        send(getId(), (estimatedFinishTime-CloudSim.clock()), CloudSimTags.VM_DATACENTER_EVENT);
////                        send(getId(), (cl.getMaxExecTime()+cl.getArrivalTime()-CloudSim.clock()), CloudSimTags.PREEMPT_request);
//                    }
//
//                }
//
//            } else {// the request will migrate from one resource to another
//                int tag = ((type == CloudSimTags.CLOUDLET_MOVE_ACK) ? CloudSimTags.CLOUDLET_SUBMIT
//                        : CloudSimTags.CLOUDLET_SUBMIT_ACK);
//                sendNow(destId, tag, cl);
//            }
//
//        }
//
//        checkCloudletCompletion();
//
//        /** Destroy idling containers*/
//        while(!containersToDestroy.isEmpty()){
//            for(int x=0; x<containersToDestroy.size(); x++){
//                if(((ServerlessContainer) containersToDestroy.get(x)).newContainer){
////                    System.out.println("Container to be destroyed removed since new: "+getContainersToDestroy().get(x).getId());
//                    getContainersToDestroy().remove(x);
//                    continue;
//                }
//                if(containersToDestroy.get(x).getId()==94){
//                    System.out.println("to be destroyed 94 at request move");
//                }
//                //System.out.println("Container to be destroyed: "+containersToDestroy.get(x).getId());
//                sendNow(this.getId(), CloudSimTags.CONTAINER_DESTROY_ACK,containersToDestroy.get(x));
//            }
//            containersToDestroy.clear();
//        }
//
//        /**Update CPU Utilization of Vm */
//        /*for(ContainerVm vm: getContainerVmList()){
//            addToCPUUtilizationLog(vm.getId(),vm.getAvailableMips()/vm.getTotalMips());
//        }*/
//
//
//        if (type == CloudSimTags.CLOUDLET_MOVE_ACK) {// send ACK if requested
//            /*int[] data = new int[4];
//            data[0] = getId();
//            data[1] = requestId;
//            data[2] = oldVm.getId();
//            data[3] = newContainerVm.getId() ;
//            if (failed) {
//                data[4] = 0;
//            } else {
//                data[4] = 1;
//            }*/
//
//            Pair data = new Pair<>(cl,oldVm.getId());
//
//            sendNow(cl.getUserId(), CloudSimTags.CLOUDLET_MOVE_ACK, data);
//        }
//    }

    public void processAutoScaling(SimEvent ev){
        Log.printLine(String.format("%s: Autoscaling", CloudSim.clock()));
        autoScaler.scaleFunctions();
        destroyIdleContainers();
        send(this.getId(), Constants.AUTO_SCALING_INTERVAL, CloudSimSCTags.AUTO_SCALE);
    }

    /** Process event to destroy a container */
    public void processContainerDestroy(SimEvent ev, boolean ack){
        Container container = (Container) ev.getData();
        if (Constants.containerIdlingEnabled){
//            if(container.getId()==1 && CloudSim.clock()>12){
//                System.out.println("debug");
//            }
//            Log.printConcatLine(CloudSim.clock(), " checking to destroy container ", container.getId());
            if (Math.round(CloudSim.clock()*100000)/100000 - Math.round(((ServerlessContainer)container).getIdleStartTime()*100000)/100000 == Constants.containerIdlingTime){
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
        }
        else {
            ServerlessInvoker vm = (ServerlessInvoker) container.getVm();
            if (vm != null) {
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

    }

    @Override

    protected void processVmCreate(SimEvent ev, boolean ack) {

        ContainerVm containerVm = (ContainerVm) ev.getData();
        Log.printLine(String.format("In processVmcreate in serverlessDC to create vm #%s", containerVm.getId()));

        boolean result = getVmAllocationPolicy().allocateHostForVm(containerVm);
//        if(containerVm.getId()==12){
//            System.out.println("d");
//        }

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

            if(Constants.monitoring) {

                send(containerVm.getUserId(), Constants.CPU_USAGE_MONITORING_INTERVAL, CloudSimSCTags.RECORD_CPU_USAGE, containerVm);
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
            result = ((FunctionScheduler)getContainerAllocationPolicy()).allocateVmForContainer(container, container.getVm(), getContainerVmList());
        } else {
            result = ((FunctionScheduler)getContainerAllocationPolicy()).allocateVmForContainer(container, getContainerVmList());
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
//            if(CloudSim.clock() > 12){
//                System.out.println("debug");
//            }
            // if some time passed since last processing
            // R: for term is to allow loop at simulation start. Otherwise, one initial
            // simulation step is skipped and schedulers are not properly initialized
            if (CloudSim.clock() < 0.111 || CloudSim.clock() > getLastProcessTime()) {
                List<? extends ContainerHost> list = getVmAllocationPolicy().getContainerHostList();
                double smallerTime = Double.MAX_VALUE;
                for (ContainerHost host : list) {
                    // inform VMs to update processing
                    double time = host.updateContainerVmsProcessing(CloudSim.clock());
                    // what time do we expect that the next request will finish?
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

                if(!Constants.functionAutoScaling){
                    destroyIdleContainers();
                }



                /**Create online bin        */
//                createOnlineVmBin();


            }
//            DC_event = false;


        }

    @Override
    protected void processCloudletSubmit(SimEvent ev, boolean ack) {
        updateCloudletProcessing();

        try {
            ServerlessRequest cl = (ServerlessRequest) ev.getData();

            // checks whether this request has finished or not
            if (cl.isFinished()) {
                String name = CloudSim.getEntityName(cl.getUserId());
                Log.printConcatLine(getName(), ": Warning - request #", cl.getCloudletId(), " owned by ", name,
                        " is already completed/finished.");
                Log.printLine("Therefore, it is not being executed again");
                Log.printLine();

                // NOTE: If a request has finished, then it won't be processed.
                // So, if ack is required, this method sends back a result.
                // If ack is not required, this method don't send back a result.
                // Hence, this might cause CloudSim to be hanged since waiting
                // for this request back.
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

            // process this request to this CloudResource
            cl.setResourceParameter(getId(), getCharacteristics().getCostPerSecond(), getCharacteristics()
                    .getCostPerBw(), cl.getVmId());

            int userId = cl.getUserId();
            int vmId = cl.getVmId();
            int containerId = cl.getContainerId();

            // time to transfer the files
            double fileTransferTime = predictFileTransferTime(cl.getRequiredFiles());
            ContainerHost host=null;
//            if(cl.getCloudletId()==1184){
//                System.out.println(CloudSim.clock());
//                host = getVmAllocationPolicy().getHost(vmId, userId);
//            }
//            else {
//                host = getVmAllocationPolicy().getHost(vmId, userId);
//            }
            host = getVmAllocationPolicy().getHost(vmId, userId);
            ServerlessInvoker vm = null;
            Container container = null;
            double estimatedFinishTime = 0;

            vm = (ServerlessInvoker)host.getContainerVm(vmId, userId);
            container = vm.getContainer(containerId, userId);

//            if(cl.getCloudletId()==14){
//                System.out.println("here");
//            }

            estimatedFinishTime =((ServerlessRequestScheduler) container.getContainerCloudletScheduler()).requestSubmit(cl, vm, (ServerlessContainer)(container));
            System.out.println("Est finish time of function# "+ cl.getCloudletId()+" at the beginning is "+ estimatedFinishTime);
            // }
//            count++;

//            /** Update Vm Bin */
//            updateOnlineVmBin((ServerlessInvoker)vm, cl.getArrivalTime()+cl.getMaxExecTime()-CloudSim.clock());
//            updateRunTimeVm((ServerlessInvoker)vm, cl.getArrivalTime()+cl.getMaxExecTime()-CloudSim.clock());
//
//            /** Send an event when 90% of the deadline is reached for a request */
////            System.out.println(CloudSim.clock()+" Deadline checkpoint time for request# "+cl.getrequestId()+" is "+(CloudSim.clock()+(cl.getArrivalTime()+cl.getMaxExecTime()-CloudSim.clock())* Constants.DEADLINE_CHECKPOINT));
//            if(cl.getPriority()==1){
//                send(getId(), ((cl.getArrivalTime()+cl.getMaxExecTime()-CloudSim.clock())* Constants.DEADLINE_CHECKPOINT_LOW), CloudSimSCTags.DEADLINE_CHECKPOINT,cl);
//            }
//            else{
//                send(getId(), ((cl.getArrivalTime()+cl.getMaxExecTime()-CloudSim.clock())* Constants.DEADLINE_CHECKPOINT_HIGH), CloudSimSCTags.DEADLINE_CHECKPOINT,cl);
//            }

            /**Remove the new request's container from removal list*/
            getContainersToDestroy().remove(container);
            //System.out.println("Submit: Removed from destroy list container "+container.getId());
            // if this request is in the exec queue
            if (estimatedFinishTime > 0.0 && !Double.isInfinite(estimatedFinishTime)) {
                estimatedFinishTime += fileTransferTime;
//                if(cl.getCloudletId()==14){
//                    System.out.println("debug");
//                }

                send(getId(), (estimatedFinishTime-CloudSim.clock()), CloudSimTags.VM_DATACENTER_EVENT);

//                /*** PREEMPTION  ***/
//                if (ack) {
//                    System.out.println("request" + cl.getCloudletId() +" in container "+ cl.getContainerId()+" is to be preempted at time "+(cl.getMaxExecTime() + cl.getArrivalTime()));
//                    send(getId(), (cl.getMaxExecTime() + cl.getArrivalTime() - CloudSim.clock()+2), CloudSimSCTags.PREEMPT_REQUEST, cl);
//                }
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
        if (Constants.functionAutoScaling && !autoScalingInitialized){
            autoScalingInitialized = true;
            autoScaler.scaleFunctions();
            destroyIdleContainers();
            send(this.getId(), Constants.AUTO_SCALING_INTERVAL, CloudSimSCTags.AUTO_SCALE);
        }



//        destroyIdleContainers();

        /**Update CPU Utilization of Vm */
        /*for(ContainerVm vm: getContainerVmList()){
            addToCPUUtilizationLog(vm.getId(),(vm.getTotalMips()-vm.getAvailableMips())/vm.getTotalMips());
        }*/



    }

    protected void sendScaledContainerCreationRequest(String[] data){
        sendNow(Integer.parseInt(data[0]), CloudSimSCTags.SCALED_CONTAINER, data);
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
//                            if(cl.getCloudletId()==15){
//                                System.out.println("debug");
//                            }
                            Pair data = new Pair<>(cl, vm);
                            for(int x=0; x<((ServerlessInvoker)vm).getRunningRequestList().size();x++){
                                if(((ServerlessInvoker)vm).getRunningRequestList().get(x)==(ServerlessRequest)cl){
                                    ((ServerlessInvoker)vm).getRunningRequestList().remove(x);
                                }
                            }

                            sendNow(cl.getUserId(), CloudSimTags.CLOUDLET_RETURN, data);
//                            removeFromVmTaskMap((ServerlessRequest)cl,(ServerlessInvoker)vm);
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
//                    getContainersToDestroy().remove(x);
                    ((ServerlessContainer)getContainersToDestroy().get(x)).setIdleStartTime(0);
                    continue;
                }
//                System.out.println(((ServerlessContainer)e.getContainersToDestroy().get(x)).newContainer);
//                    System.out.println("Container to be destroyed: " + e.getContainersToDestroy().get(x).getId());
                if(!Constants.containerIdlingEnabled){
                    sendNow(getId(), CloudSimTags.CONTAINER_DESTROY_ACK, getContainersToDestroy().get(x));
                }
                else {
//                    if(((ServerlessContainer)getContainersToDestroy().get(x)).getId()==1 && CloudSim.clock() >12){
//                        Log.print("destroy cotnainer "+ String.valueOf(((ServerlessContainer)getContainersToDestroy().get(x)).getId())+ "at "+ String.valueOf(CloudSim.clock()+Constants.containerIdlingTime));
//                    }
                    send(getId(), Constants.containerIdlingTime, CloudSimTags.CONTAINER_DESTROY_ACK, getContainersToDestroy().get(x));
                }

            }
            getContainersToDestroy().clear();
        }

    }

    public void updateRunTimeVm(ServerlessInvoker vm, double time){
        if(runTimeVm.get(vm.getId())<time){
            runTimeVm.put(vm.getId(),time);
        }
    }

//    /**Creates online bin        */
//    public void createOnlineVmBin(){
////        System.out.println("PRINTING RUNTIMEVM: "+runTimeVm);
//        onlineBinOfVms.clear();
//        binNos.clear();
//        for (Map.Entry<Integer, Double> entry : runTimeVm.entrySet()) {
//
//            if(entry.getValue()>1) {
////                System.out.println(">>>>>*******************Debug: Bin added ");
//                int bin = (int) (Math.ceil(Math.log(entry.getValue()) / Math.log(2)));
//
//                if(!onlineBinOfVms.containsKey(bin)) {
//                    ArrayList<Integer> instanceIds = new ArrayList<>();
//                    instanceIds.add(entry.getKey());
//                    onlineBinOfVms.put(bin, instanceIds);
//                }
//                else{
//                    onlineBinOfVms.get(bin).add(entry.getKey());
//                }
//                if(!binNos.contains(bin)) {
//                    binNos.add(bin);
//                }
////                System.out.println(CloudSim.clock()+" Bin "+bin+" is added to binArray");
//                Collections.sort(binNos);
//            }
//            else if(entry.getValue()>0 && entry.getValue()<1){
//                int bin = 1;
//                if(!onlineBinOfVms.containsKey(bin)) {
//                    ArrayList<Integer> instanceIds = new ArrayList<>();
//                    instanceIds.add(entry.getKey());
//                    onlineBinOfVms.put(bin, instanceIds);
//                }
//                else{
//                    onlineBinOfVms.get(bin).add(entry.getKey());
//                }
//                if(!binNos.contains(bin)) {
//                    binNos.add(bin);
//                }
//
////                System.out.println(CloudSim.clock()+" Bin "+bin+" is added to binArray");
//                Collections.sort(binNos);
//            }
//            else if(entry.getValue()==0 ){
//                int bin = 0;
//                if(!onlineBinOfVms.containsKey(bin)) {
//                    ArrayList<Integer> instanceIds = new ArrayList<>();
//                    instanceIds.add(entry.getKey());
//                    onlineBinOfVms.put(bin, instanceIds);
//                }
//                else{
//                    onlineBinOfVms.get(bin).add(entry.getKey());
//                }
//                if(!binNos.contains(bin)) {
//                    binNos.add(bin);
//                }
//
////                System.out.println(CloudSim.clock()+" Bin "+bin+" is added to binArray");
//                Collections.sort(binNos);
//            }
//        }
//
//    }
//
//    /**Updates online bin        */
//    public void updateOnlineVmBin(ServerlessInvoker vm, double time){
//        if(runTimeVm.get(vm.getId())<time){
//            if(runTimeVm.get(vm.getId())>1) {
//                int oldBin = (int) (Math.ceil(Math.log(runTimeVm.get(vm.getId())) / Math.log(2)));
////                System.out.println(("Debug: Old bin: "+ oldBin));
//
//                if(onlineBinOfVms.containsKey(oldBin)) {
//
//                    onlineBinOfVms.get(oldBin).remove(new Integer(vm.getId()));
//                    if ((onlineBinOfVms.get(oldBin)).size() ==0) {
//                        onlineBinOfVms.remove(oldBin);
//
//                        for(int x=0; x<binNos.size(); x++){
//                            if(binNos.get(x)==oldBin){
//                                binNos.remove(x);
//                                break;
//                            }
//
//                        }
//
//                    }
//                }
//
//
//
////                System.out.println("Update bin time "+time);
//                int newBin = (int) (Math.ceil(Math.log(time) / Math.log(2)));
////                System.out.println("Debug: New bin: " + newBin);
//                if(onlineBinOfVms.containsKey(newBin)){
//                    onlineBinOfVms.get(newBin).add(vm.getId());
//                }
//                else{
////                    System.out.println("New bin added: New bin "+newBin+" "+"with vm "+vm.getId());
//                    ArrayList<Integer> instanceIds = new ArrayList<>();
//                    instanceIds.add(vm.getId());
//                    onlineBinOfVms.put(newBin,instanceIds);
//                    if(!binNos.contains(newBin)) {
//                        binNos.add(newBin);
//                    }
////                    System.out.println(CloudSim.clock()+" Bin "+newBin+" is added to binArray");
//                    Collections.sort(binNos);
//
//                }
//
//            }
//            else if(runTimeVm.get(vm.getId())>0 && runTimeVm.get(vm.getId())<1){
//                int oldBin = 1;
////                System.out.println(("Debug: Old bin: "+ oldBin));
//                if(onlineBinOfVms.containsKey(oldBin)) {
//
//                    onlineBinOfVms.get(oldBin).remove(new Integer(vm.getId()));
//                    if ((onlineBinOfVms.get(oldBin)).size() ==0) {
//                        onlineBinOfVms.remove(oldBin);
//                        for(int x=0; x<binNos.size(); x++){
//                            if(binNos.get(x)==oldBin){
//                                binNos.remove(x);
//                                break;
//                            }
//
//                        }
//                    }
//                }
//
////                System.out.println("Update bin time "+time);
//                if(time>1){
//                    int newBin = (int) (Math.ceil(Math.log(time) / Math.log(2)));
////                    System.out.println("Debug: New bin: " + newBin);
//                    if(onlineBinOfVms.containsKey(newBin)){
//                        onlineBinOfVms.get(newBin).add(vm.getId());
//                    }
//                    else{
////                        System.out.println("New bin added: New bin "+newBin+" "+"with vm "+vm.getId());
//                        ArrayList<Integer> instanceIds = new ArrayList<>();
//                        instanceIds.add(vm.getId());
//                        onlineBinOfVms.put(newBin,instanceIds);
//                        if(!binNos.contains(newBin)) {
//                            binNos.add(newBin);
//                        }
////                        System.out.println(CloudSim.clock()+" Bin "+newBin+" is added to binArray");
//                        Collections.sort(binNos);
//
//                    }
//                }
//                else{
//                    int newBin = 1;
////                    System.out.println("Debug: New bin: " + newBin);
//                    if(onlineBinOfVms.containsKey(newBin)){
//                        onlineBinOfVms.get(newBin).add(vm.getId());
//                    }
//                    else{
////                        System.out.println("New bin added: New bin "+newBin+" "+"with vm "+vm.getId());
//                        ArrayList<Integer> instanceIds = new ArrayList<>();
//                        instanceIds.add(vm.getId());
//                        onlineBinOfVms.put(newBin,instanceIds);
//                        if(!binNos.contains(newBin)) {
//                            binNos.add(newBin);
//                        }
////                        System.out.println(CloudSim.clock()+" Bin "+newBin+" is added to binArray");
//                        Collections.sort(binNos);
//
//                    }
//
//                }
//
//
//            }
//            else if(runTimeVm.get(vm.getId())==0 ){
//                int oldBin = 0;
////                System.out.println(("Debug: Old bin: "+ oldBin));
//                if(onlineBinOfVms.containsKey(oldBin)) {
//
//                    onlineBinOfVms.get(oldBin).remove(new Integer(vm.getId()));
//                    if ((onlineBinOfVms.get(oldBin)).size() ==0) {
//                        onlineBinOfVms.remove(oldBin);
//                        for(int x=0; x<binNos.size(); x++){
//                            if(binNos.get(x)==oldBin){
//                                binNos.remove(x);
//                                break;
//                            }
//
//                        }
//                    }
//                }
//
////                System.out.println("Update bin time "+time);
//                if(time>1){
//                    int newBin = (int) (Math.ceil(Math.log(time) / Math.log(2)));
////                    System.out.println("Debug: New bin: " + newBin);
//                    if(onlineBinOfVms.containsKey(newBin)){
//                        onlineBinOfVms.get(newBin).add(vm.getId());
//                    }
//                    else{
////                        System.out.println("New bin added: New bin "+newBin+" "+"with vm "+vm.getId());
//                        ArrayList<Integer> instanceIds = new ArrayList<>();
//                        instanceIds.add(vm.getId());
//                        onlineBinOfVms.put(newBin,instanceIds);
//                        if(!binNos.contains(newBin)) {
//                            binNos.add(newBin);
//                        }
////                        System.out.println(CloudSim.clock()+" Bin "+newBin+" is added to binArray");
//                        Collections.sort(binNos);
//
//                    }
//                }
//                else{
//                    int newBin = 1;
////                    System.out.println("Debug: New bin: " + newBin);
//                    if(onlineBinOfVms.containsKey(newBin)){
//                        onlineBinOfVms.get(newBin).add(vm.getId());
//                    }
//                    else{
////                        System.out.println("New bin added: New bin "+newBin+" "+"with vm "+vm.getId());
//                        ArrayList<Integer> instanceIds = new ArrayList<>();
//                        instanceIds.add(vm.getId());
//                        onlineBinOfVms.put(newBin,instanceIds);
//                        if(!binNos.contains(newBin)) {
//                            binNos.add(newBin);
//                        }
////                        System.out.println(CloudSim.clock()+" Bin "+newBin+" is added to binArray");
//                        Collections.sort(binNos);
//
//                    }
//
//                }
//
//
//            }
//
//
//
//        }
//
//    }

}


