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

            // other unknown tags are processed by this method
            default:
                super.processOtherEvent(ev);
                break;
        }
    }


    public void containerVerticalScale(Container container, ServerlessInvoker vm, int cpuChange, int memChange){
        boolean result = ((FunctionScheduler) getContainerAllocationPolicy()).reallocateVmResourcesForContainer(container, vm, cpuChange, memChange);

    }


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


            /**Remove the new request's container from removal list*/
            getContainersToDestroy().remove(container);
            //System.out.println("Submit: Removed from destroy list container "+container.getId());
            // if this request is in the exec queue
            if (estimatedFinishTime > 0.0 && !Double.isInfinite(estimatedFinishTime)) {
                estimatedFinishTime += fileTransferTime;

                send(getId(), (estimatedFinishTime-CloudSim.clock()), CloudSimTags.VM_DATACENTER_EVENT);


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


}


