package org.cloudbus.cloudsim.serverless;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
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

    }

    /**
     * Get the idle containers that need to be destroyed
     */
    public List<Container> getContainersToDestroy() {
        return containersToDestroy;
    }


    public void setMonitoring(boolean monitor) {
        this.monitoring = monitor;
    }

    public boolean getMonitoring() {
        return monitoring;
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

    /** Process event to destroy a container */
    public void processContainerDestroy(SimEvent ev, boolean ack){
        Container container = (Container) ev.getData();
        ServerlessInvoker vm = (ServerlessInvoker)container.getVm();
        if(vm!=null) {
            getContainerAllocationPolicy().deallocateVmForContainer(container);
            if (container.getId() == 94) {
                System.out.println("debug");
            }

            /** Add vm to idle list if there ar eno more containers */
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

                if (getMonitoring()) {
                    /** Remove vm from idle list when first container is created */
                    if ((container.getVm()).getContainerList().size() == 1) {
                        vmIdleList.remove(container.getVm());
                        ServerlessInvoker vm = ((ServerlessInvoker) container.getVm());
                        vm.setStatus("ON");
                        vm.offTime += (CloudSim.clock() - vm.getRecordTime());
                        vm.setRecordTime(CloudSim.clock());

                            /*double outTimeRecorded = (vm.getVmUpTime()).get("Out");
                             (vm.getVmUpTime()).put("Out",outTimeRecorded+CloudSim.clock()-vm.outTime);*/
                        //                         vm.outTime = CloudSim.clock();
                        //                         vm.inTime = CloudSim.clock();
                    }
                    ContainerVm containerVm = getContainerAllocationPolicy().getContainerVm(container);
                    data[0] = containerVm.getId();
                    if (containerVm.getId() == -1) {

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
                    ((ServerlessContainer) container).updateContainerProcessing(CloudSim.clock(), getContainerAllocationPolicy().getContainerVm(container).getContainerScheduler().getAllocatedMipsForContainer(container), (ServerlessInvoker) containerVm);
                } else {
                    data[0] = -1;
                    //notAssigned.add(container);
                    Log.printLine(String.format("Couldn't find a vm to host the container #%s", container.getUid()));

                }
                send(ev.getSource(), Constants.CONTAINER_STARTTUP_DELAY, containerCloudSimTags.CONTAINER_CREATE_ACK, data);

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

            estimatedFinishTime =((ServerlessCloudletScheduler) container.getContainerCloudletScheduler()).cloudletSubmit(cl, fileTransferTime,vm);
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
        destroyIdleContainers();

        /**Update CPU Utilization of Vm */
        /*for(ContainerVm vm: getContainerVmList()){
            addToCPUUtilizationLog(vm.getId(),(vm.getTotalMips()-vm.getAvailableMips())/vm.getTotalMips());
        }*/



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


