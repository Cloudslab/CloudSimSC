package org.cloudbus.cloudsim.serverless;

import javafx.util.Pair;
import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.container.core.*;
import org.cloudbus.cloudsim.container.lists.ContainerList;
import org.cloudbus.cloudsim.container.lists.ContainerVmList;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;

import java.util.*;

/**
 * Broker class for CloudSimServerless extension. This class represents a broker (Service Provider)
 * who uses the Cloud data center.
 *
 * @author Anupama Mampage
 */

public class ServerlessController extends ContainerDatacenterBroker {

    protected List<ServerlessInvoker> vmIdleList = new ArrayList<>();
    protected List<Container> containerList = new ArrayList<Container>();
    /**
     * The containers destroyed list.
     */
    protected List<? extends ServerlessContainer> containerDestroyedList;
    protected List<ServerlessRequest> requestList = new ArrayList<ServerlessRequest>();
    private static int overBookingfactor = 0;
    /**
     * The arrival time of each serverless request
     */
    public Queue<Double> requestArrivalTime = new LinkedList<Double>();
    /**
     * The serverless function requests queue
     */
    public Queue<ServerlessRequest> requestQueue = new LinkedList<ServerlessRequest>();
    /**
     * The task type and vm map of controller - contains the list of vms running each function type
     */
    protected Map<String, ArrayList<ServerlessInvoker>> functionVmMap = new HashMap<String, ArrayList<ServerlessInvoker>>();
    protected List<ServerlessRequest> toSubmitOnContainerCreation = new ArrayList<ServerlessRequest>();
    protected List<Double> averageVmUsageRecords = new ArrayList<Double>();
    protected List<Double> meanAverageVmUsageRecords = new ArrayList<Double>();
    protected List<Integer> vmCountList = new ArrayList<Integer>();
    protected List<Double> meanSumOfVmCount = new ArrayList<Double>();
    protected double timeInterval = 50.0;
    protected double requestSubmitClock = 0;
    protected Map<ServerlessInvoker, ArrayList<ServerlessRequest>> vmTempTimeMap = new HashMap<ServerlessInvoker,ArrayList<ServerlessRequest>>();
    ServerlessDatacenter e ;

    /**
     * The loadBalancer ID.
     */
    private RequestLoadBalancer loadBalancer;

    public int controllerId=0;
    public int containerId = 1;;
    private boolean reschedule = false;
    int dcount = 1;
    public int exsitingContCount = 0;
    public int noOfTasks = 0;
    public int noOfTasksReturned = 0;
    private String vmSelectionMode= "RR";
    private String subMode= "NEWBM";
    /** Vm index for selecting Vm in round robin fashion **/
    private int selectedVmIndex = 1;
    /** The map of tasks to reschedule with the new containerId. */
    private static Map<ServerlessRequest, Integer> tasksToReschedule = new HashMap<>();

    @Override
    protected void processOtherEvent(SimEvent ev){
        switch (ev.getTag()) {
            case CloudSimTags.CLOUDLET_SUBMIT:
//                processrequestSubmit(ev);
                submitRequest(ev);
                break;
            case CloudSimTags.CLOUDLET_SUBMIT_ACK:
                processRequestSubmitAck(ev);
                break;
            // other unknown tags are processed by this method
            case CloudSimTags.CONTAINER_DESTROY_ACK:
                processContainerDestroy(ev);
                break;
            case CloudSimSCTags.SCALED_CONTAINER:
                processScaledContainer(ev);
                break;
            case CloudSimSCTags.RECORD_CPU_USAGE:
                processRecordCPUUsage(ev);
                break;
            default:
                super.processOtherEvent(ev);
                break;
        }
    }

    public ServerlessController(String name, int overBookingfactor) throws Exception {
        super(name, overBookingfactor);
        ServerlessController.overBookingfactor = overBookingfactor;
        setContainersDestroyedList(new ArrayList<ServerlessContainer>());
//        createRequests();
    }

    @Override
    public void startEntity() {
        super.startEntity();
        while(!requestArrivalTime.isEmpty()){
            send(getId(), requestArrivalTime.remove(), CloudSimTags.CLOUDLET_SUBMIT,requestQueue.remove());

        }
    }

    public <T extends ServerlessContainer> List<T> getContainersDestroyedList() {
        return (List<T>) containerDestroyedList;
    }

    /**
     * Sets the container destroyed list.
     *
     * @param <T>                  the generic type
     * @param containerDestroyedList the new cloudlet received list
     */
    protected <T extends ServerlessContainer> void setContainersDestroyedList(List<T> containerDestroyedList) {
        this.containerDestroyedList = containerDestroyedList;
    }

    public void setLoadBalancer(RequestLoadBalancer lb) {
        this.loadBalancer = lb;
    }
    public void setServerlessDatacenter(ServerlessDatacenter dc) {
        this.e = dc;
    }

    public RequestLoadBalancer getLoadBalancer() {
        return loadBalancer;
    }

    public void submitRequest(SimEvent ev) {
        ServerlessRequest cl = (ServerlessRequest) ev.getData();
        System.out.println(CloudSim.clock() + " request arrived: " + cl.getCloudletId());
        if (CloudSim.clock() == requestSubmitClock) {
            send(getId(), Constants.MINIMUM_INTERVAL_BETWEEN_TWO_CLOUDLET_SUBMISSIONS, CloudSimTags.CLOUDLET_SUBMIT, cl);
        }
        else {
            e.updateCloudletProcessing();
            submitRequestToList(cl);
            loadBalancer.routeRequest(cl);
        }

    }

    protected void sendFunctionRetryRequest(ServerlessRequest req){
        send(getId(), Constants.FUNCTION_SCHEDULING_RETRY_DELAY, CloudSimTags.CLOUDLET_SUBMIT, req);
    }



    protected void createContainer(ServerlessRequest cl, String requestId, int brokerId) {
//        double containerMips = 0;
/**     container MIPS is set as specified for that container type  **/
//        containerMips = Constants.CONTAINER_MIPS[Integer.parseInt(requestId)];
        ServerlessContainer container = new ServerlessContainer(containerId, brokerId, requestId, cl.getContainerMIPS(), cl.getNumberOfPes(), cl.getContainerMemory(), Constants.CONTAINER_BW, Constants.CONTAINER_SIZE,"Xen", new ServerlessRequestScheduler(cl.getContainerMIPS(), cl.getNumberOfPes()), Constants.SCHEDULING_INTERVAL, true, false, false, 0, 0, 0);
        getContainerList().add(container);
        if (!(cl ==null)){
            cl.setContainerId(containerId);
        }

        submitContainer(cl, container);
        containerId++;


    }
    protected void processScaledContainer(SimEvent ev){
        String[] data = (String[]) ev.getData();
        int brokerId = Integer.parseInt(data[0]);
        String requestId = data[1];
        double containerMips = Double.parseDouble(data[2]);
        int containerRAM = (int)Double.parseDouble(data[3]);
        int containerPES = (int)Double.parseDouble(data[4]);
        if(containerId==52 ){
            System.out.println("debug");
        }
        ServerlessContainer container = new ServerlessContainer(containerId, brokerId, requestId, containerMips, containerPES, containerRAM, Constants.CONTAINER_BW, Constants.CONTAINER_SIZE,"Xen", new ServerlessRequestScheduler(containerMips, containerPES), Constants.SCHEDULING_INTERVAL, true, false, false, 0, 0, 0);
        getContainerList().add(container);
        container.setWorkloadMips(container.getMips());
        sendNow(getDatacenterIdsList().get(0), containerCloudSimTags.CONTAINER_SUBMIT, container);
        Log.printLine(String.format("clock %s Creating scaled container: container #%s",CloudSim.clock(), container.getId()));

        containerId++;

    }

    protected void submitContainer(ServerlessRequest cl, Container container){
        container.setWorkloadMips(container.getMips());
//        if(cl.getReschedule()){
//            sendNow(getDatacenterIdsList().get(0), containerCloudSimTags.CONTAINER_SUBMIT_FOR_RESCHEDULE, container);
//        }
//        else
        sendNow(getDatacenterIdsList().get(0), containerCloudSimTags.CONTAINER_SUBMIT, container);

    }

    protected void addToVmTaskMap(ServerlessRequest task, ServerlessInvoker vm){
        int count = vm.getvmTaskMap().getOrDefault(task.getRequestFunctionId(), 0);
        vm.getvmTaskMap().put(task.getRequestFunctionId(), count+1);

    }
    public void removeFromVmTaskMap(ServerlessRequest task, ServerlessInvoker vm){
        //System.out.println("Trying to remove from map task # "+task.getrequestId()+"Now task map of VM: "+vm.getId()+" "+ vm.getvmTaskMap());
        int count = vm.getvmTaskMap().get(task.getRequestFunctionId());
        vm.getvmTaskMap().put(task.getRequestFunctionId(), count-1);
        if(count==1){
            functionVmMap.get(task.getRequestFunctionId()).remove(vm);
            if(functionVmMap.get(task.getRequestFunctionId())==null){
                functionVmMap.remove(task.getRequestFunctionId());
            }
        }
    }

    public void removeFromVmTaskExecutionMap(ServerlessRequest task, ServerlessInvoker vm){
        vm.getVmTaskExecutionMap().get(task.getRequestFunctionId()).remove(task);
        vm.getVmTaskExecutionMapFull().get(task.getRequestFunctionId()).remove(task);
    }

    public void submitRequestToList(ServerlessRequest request) {
        getCloudletList().add(request);
    }


    @Override
    /*process requests to submit after container creation*/
    public void processContainerCreate(SimEvent ev) {
        int[] data = (int[]) ev.getData();
        int vmId = data[0];
        int containerId = data[1];
        int result = data[2];

//        reschedule = data[3] == 1;

        ServerlessContainer cont = ContainerList.getById(getContainerList(), containerId);

        //System.out.println(">>>>>>Container list size: "+getContainerList().size()+" container MIPS "+ ContainerList.getById(getContainerList(), containerId).getCurrentRequestedMips());
        if (result == CloudSimTags.TRUE) {
            if(vmId ==-1){
                Log.printConcatLine("Error : Where is the VM");}
            else{
                getContainersToVmsMap().put(containerId, vmId);
//                getContainerList().remove(ContainerList.getById(getContainerList(), containerId));
                getContainersCreatedList().add(cont);
                cont.setStartTime(CloudSim.clock());
                ServerlessInvoker vm = (ServerlessInvoker)(ContainerVmList.getById(getVmsCreatedList(),vmId));
                vm.getFunctionContainerMapPending().get(cont.getType()).remove(cont);
                vm.setFunctionContainerMap(cont, cont.getType());

                int hostId = ContainerVmList.getById(getVmsCreatedList(), vmId).getHost().getId();
                Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": The Container #", containerId,
                        ", is created on Vm #",vmId
                        , ", On Host#", hostId);
                setContainersCreated(getContainersCreated()+1);}
        } else {
            //Container container = ContainerList.getById(getContainerList(), containerId);
            Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Failed Creation of Container #", containerId);
        }

        incrementContainersAcks();
        //Log.print(String.format("%f:Broker:  containeracks: %d  and contlistsize: %d \n", CloudSim.clock(), getContainersAcks(),getContainerList().size()));
        List<ServerlessRequest> toRemove = new ArrayList<>();
        if (!toSubmitOnContainerCreation.isEmpty()) {
            for(ServerlessRequest request: toSubmitOnContainerCreation){
                if(request.getContainerId()==containerId) {
                    ServerlessInvoker vm = (ServerlessInvoker)(ContainerVmList.getById(getVmsCreatedList(),vmId));
                    if(vm!=null) {
                        addToVmTaskMap(request, vm);
                        vmTempTimeMap.get(vm).remove(request);
                        vm.setFunctionContainerMap(cont, request.getRequestFunctionId());
                        setFunctionVmMap(((ServerlessInvoker) (ContainerVmList.getById(getVmsCreatedList(), vmId))), request.getRequestFunctionId());
                        submitRequestToDC(request, vmId, 0, containerId);

                        toRemove.add(request);
                    }
                }
            }
            //Log.print(getContainersCreatedList().size() + "vs asli"+getContainerList().size());

            toSubmitOnContainerCreation.removeAll(toRemove);
            toRemove.clear();
        }

        dcount++;



    }

    @Override
    protected void processVmCreate(SimEvent ev) {
        int[] data = (int[]) ev.getData();
        int datacenterId = data[0];
        int vmId = data[1];
        int result = data[2];

        if (result == CloudSimTags.TRUE) {
            getVmsToDatacentersMap().put(vmId, datacenterId);
            getVmsCreatedList().add(ContainerVmList.getById(getVmList(), vmId));

            /** Add Vm to idle list */
            vmIdleList.add(ContainerVmList.getById(getVmList(), vmId));
            ArrayList<ServerlessRequest> taskList = new ArrayList<>();
            vmTempTimeMap.put(ContainerVmList.getById(getVmList(), vmId),taskList);
            Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": VM #", vmId,
                    " has been created in Datacenter #", datacenterId, ", Host #",
                    ContainerVmList.getById(getVmsCreatedList(), vmId).getHost().getId());
            setNumberOfCreatedVMs(getNumberOfCreatedVMs() + 1);
        } else {
            Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Creation of VM #", vmId,
                    " failed in Datacenter #", datacenterId);
        }

        incrementVmsAcks();

    }

    public void submitRequestToDC(ServerlessRequest request, int vmId, double delay, int containerId){
//        if(!request.getReschedule()) {
        request.setVmId(vmId);
        cloudletsSubmitted++;
        getCloudletSubmittedList().add(request);
        getCloudletList().remove(request);
        //System.out.println("Time " + request.getMaxExecTime());


        Log.print(String.format("%f: request %d has been submitted to VM %d and container %d", CloudSim.clock(), request.getCloudletId(), vmId, containerId));
        if (delay > 0) {
            send(getVmsToDatacentersMap().get(request.getVmId()), delay, CloudSimTags.CLOUDLET_SUBMIT_ACK, request);
        } else
            sendNow(getVmsToDatacentersMap().get(request.getVmId()), CloudSimTags.CLOUDLET_SUBMIT_ACK, request);


    }

    public void processRequestSubmitAck (SimEvent ev){
        ServerlessRequest task = (ServerlessRequest) ev.getData();
        Container cont = ContainerList.getById(getContainersCreatedList(), task.getContainerId());
        ServerlessInvoker vm = ContainerVmList.getById(getVmsCreatedList(),task.getVmId());
        ((ServerlessContainer)cont).newContainer =false;
    }


    public void setFunctionVmMap(ServerlessInvoker vm, String functionId){
        if(!functionVmMap.containsKey(functionId)){
            ArrayList<ServerlessInvoker> vmListMap = new ArrayList<>();
            vmListMap.add(vm);
            functionVmMap.put(functionId,vmListMap);
            //System.out.println(CloudSim.clock()+" Debug: Broker: Vms with task type # "+functionId+" "+ functionVmMap.get(functionId));
        }
        else{

            if(!functionVmMap.get(functionId).contains(vm)){
                functionVmMap.get(functionId).add(vm);
            }

        }
    }

    public void processContainerDestroy(SimEvent ev) {
        int[] data = (int[]) ev.getData();
        int DCId = data[0];
        int containerId = data[1];
        //System.out.println(containerId);
        int result = data[2];
        int oldVmId = data[3];

        System.out.println(CloudSim.clock()+" Broker: Debug: Container "+ containerId+" is destroyed");

        if (result == CloudSimTags.TRUE) {

            /** If no more containers, add vm to idle list */
            if((ContainerVmList.getById(getVmsCreatedList(),oldVmId)).getContainerList().size()==0){
                vmIdleList.add((ContainerVmList.getById(getVmsCreatedList(),oldVmId)));
            }

            getContainersToVmsMap().remove(containerId);
            getContainersCreatedList().remove(ContainerList.getById(getContainersCreatedList(), containerId));
            getContainersDestroyedList().add(ContainerList.getById(getContainerList(), containerId));
            ((ServerlessContainer)(ContainerList.getById(getContainerList(), containerId))).setFinishTime(CloudSim.clock());
            setContainersCreated(getContainersCreated()-1);
        }else{
            Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Failed to destroy Container #", containerId);
        }

    }

    @Override

    protected void processCloudletReturn(SimEvent ev) {

        Pair data = (Pair) ev.getData();
        ContainerCloudlet request = (ContainerCloudlet) data.getKey();
//        if(request.getCloudletId()==11){
//            System.out.println("Debug");
//        }
        ContainerVm vm = (ContainerVm) data.getValue();
        ServerlessContainer container = (ServerlessContainer)(ContainerList.getById(getContainerList(), request.getContainerId()));

        removeFromVmTaskMap((ServerlessRequest)request,(ServerlessInvoker)vm);
        removeFromVmTaskExecutionMap((ServerlessRequest)request,(ServerlessInvoker)vm);
        ServerlessRequestScheduler clScheduler = (ServerlessRequestScheduler) (container.getContainerCloudletScheduler());
        clScheduler.deAllocateResources((ServerlessRequest) request);

        getCloudletReceivedList().add(request);
        (((ServerlessContainer)(ContainerList.getById(getContainerList(), request.getContainerId()))).getRunningTasks()).remove(request);
        ((ServerlessContainer)(ContainerList.getById(getContainerList(), request.getContainerId()))).setfinishedTask((ServerlessRequest)request);
        Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": request ", request.getCloudletId(),
                " returned");
        //Log.printConcatLine(CloudSim.clock(), ": ", getName(), "The number of finished requests is:", getrequestReceivedList().size());
        /**Debugger */
        //System.out.println(CloudSim.clock()+" Debugger: requestlist size: "+getrequestList().size()+" requestssubmitted: "+requestsSubmitted );
        cloudletsSubmitted--;


        noOfTasksReturned++;

    }


    public void processRecordCPUUsage(SimEvent ev){

        double utilization   = 0;
        int vmCount = 0;
        double sum=0;

        for(int x=0; x< getVmsCreatedList().size(); x++){
            utilization   = 1 - getVmsCreatedList().get(x).getAvailableMips() / getVmsCreatedList().get(x).getTotalMips();
            if(utilization>0){
                ((ServerlessInvoker)getVmsCreatedList().get(x)).used = true;
                vmCount++;
                sum += utilization;
            }
        }
        if(sum>0){
            averageVmUsageRecords.add(sum/vmCount);
            vmCountList.add(vmCount);
        }

        double sumOfAverage = 0;
        double sumOfVmCount = 0;
        if(averageVmUsageRecords.size()==Constants.CPU_HISTORY_LENGTH){
            for(int x=0; x<Constants.CPU_HISTORY_LENGTH; x++){
                sumOfAverage += averageVmUsageRecords.get(x);
                sumOfVmCount += vmCountList.get(x);
            }
            meanAverageVmUsageRecords.add(sumOfAverage/Constants.CPU_HISTORY_LENGTH);
            meanSumOfVmCount.add(sumOfVmCount/Constants.CPU_HISTORY_LENGTH);
            averageVmUsageRecords.clear();
            vmCountList.clear();
        }


        send(this.getId(), Constants.CPU_USAGE_MONITORING_INTERVAL, CloudSimSCTags.RECORD_CPU_USAGE);


    }

    public double getAverageResourceUtilization(){
        double averageUtilization = 0;
        double sumAverage=0;

        for(int x=0; x<meanAverageVmUsageRecords.size(); x++){
            sumAverage += meanAverageVmUsageRecords.get(x);

        }
        return (sumAverage/meanAverageVmUsageRecords.size());
    }

    public double getAverageVmCount (){
        double sumCount = 0;
        for(int x=0; x<meanSumOfVmCount.size(); x++){
            sumCount += meanSumOfVmCount.get(x);

        }
        return sumCount/meanSumOfVmCount.size();
    }


}
