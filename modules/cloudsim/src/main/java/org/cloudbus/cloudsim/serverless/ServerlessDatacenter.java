package org.cloudbus.cloudsim.serverless;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.container.core.*;
import org.cloudbus.cloudsim.container.resourceAllocators.ContainerAllocationPolicy;
import org.cloudbus.cloudsim.container.resourceAllocators.ContainerVmAllocationPolicy;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ServerlessDatacenter extends PowerContainerDatacenterCM {

    /**
     * Cloudlets to be rescheduled
     */
    private final Map<Integer,ServerlessTasks> tasksWaitingToReschedule;

    /**
     * Idle Vm list
     */
    private static List<ServerlessInvoker> vmIdleList = new ArrayList<>();

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

    public ServerlessDatacenter(String name, ContainerDatacenterCharacteristics characteristics, ContainerVmAllocationPolicy vmAllocationPolicy, ContainerAllocationPolicy containerAllocationPolicy, List<Storage> storageList, double schedulingInterval, String experimentName, String logAddress, double vmStartupDelay, double containerStartupDelay) throws Exception {
        super(name, characteristics, vmAllocationPolicy, containerAllocationPolicy, storageList, schedulingInterval, experimentName, logAddress, vmStartupDelay, containerStartupDelay);
        tasksWaitingToReschedule = new HashMap<Integer,ServerlessTasks>();

    }

    @Override
    protected void processOtherEvent(SimEvent ev) {
        switch (ev.getTag()) {
            case CloudSimTags.DEADLINE_CHECKPOINT:
                processDeadlineCheckpoint(ev,false);
                break;
            case CloudSimTags.CONTAINER_DESTROY:
                processContainerDestroy(ev,false);
                break;

            case CloudSimTags.CONTAINER_DESTROY_ACK:
                processContainerDestroy(ev,true);
                break;

//            case containerCloudSimTags.CONTAINER_SUBMIT_FOR_RESCHEDULE:
//                reschedule = true;
//                processContainerSubmit(ev, true);
//                break;

            case CloudSimTags.PREEMPT_CLOUDLET:

                preemptCloudlet(ev);
                break;


            // other unknown tags are processed by this method
            default:
                super.processOtherEvent(ev);
                break;
        }
    }

    @Override
    public void processContainerSubmit(SimEvent ev, boolean ack) {
        Container container= (Container) ev.getData();
        boolean result = false;

        /** Directly allocate resources to container if a vm is assigned already */
        if(container.getVm()!=null) {
            result = getContainerAllocationPolicy().allocateVmForContainer(container, container.getVm(), getContainerVmList());
        }
        else{
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

                /** Remove vm from idle list when first container is created */
                if((container.getVm()).getContainerList().size()==1){
                    vmIdleList.remove(container.getVm());
                    ServerlessInvoker vm = ((ServerlessInvoker)container.getVm());
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
                if(containerVm.getId() == -1){

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
                ((ServerlessContainer) container).updateContainerProcessing(CloudSim.clock(), getContainerAllocationPolicy().getContainerVm(container).getContainerScheduler().getAllocatedMipsForContainer(container), (ServerlessInvoker)containerVm);
            } else {
                data[0] = -1;
                //notAssigned.add(container);
                Log.printLine(String.format("Couldn't find a vm to host the container #%s", container.getUid()));

            }
            send(ev.getSource(), Constants.CONTAINER_STARTTUP_DELAY,containerCloudSimTags.CONTAINER_CREATE_ACK, data);

        }

    }

    @Override
    protected void updateCloudletProcessing() {
        // if some time passed since last processing
        // R: for term is to allow loop at simulation start. Otherwise, one initial
        // simulation step is skipped and schedulers are not properly initialized
        if (CloudSim.clock() < 0.111 || CloudSim.clock() > getLastProcessTime() ) {
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
            if(DC_event==true){
                while(!containersToDestroy.isEmpty()){
                    for(int x=0; x<containersToDestroy.size(); x++){
//                        System.out.println(CloudSim.clock()+"Containers to destroy "+containersToDestroy.get(x).getId());
                        if(((ServerlessContainer)containersToDestroy.get(x)).newContainer==true){
//                            System.out.println("Container to be destroyed removed since new: "+containersToDestroy.get(x).getId());
                            containersToDestroy.remove(x);

                            continue;
                        }
                        if(containersToDestroy.get(x).getId()==94){
                            System.out.println("to be destroyed 94 at update processing");
                        }

//                        System.out.println("Container to be destroyed: "+containersToDestroy.get(x).getId());
                        sendNow(this.getId(), CloudSimTags.CONTAINER_DESTROY_ACK,containersToDestroy.get(x));
                    }
                    containersToDestroy.clear();
                }

                /**Update CPU Utilization of Vm */
                /*for(ContainerVm vm: getContainerVmList()){
                    addToCPUUtilizationLog(vm.getId(),vm.getAvailableMips()/vm.getTotalMips());
                }*/
            }

            /**Create online bin        */
            createOnlineVmBin();


        }
        DC_event=false;


    }
}
