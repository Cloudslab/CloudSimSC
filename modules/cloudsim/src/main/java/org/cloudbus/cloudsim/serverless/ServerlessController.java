package org.cloudbus.cloudsim.serverless;

import com.opencsv.CSVWriter;
import javafx.util.Pair;
import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerBwProvisionerSimple;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerPe;
import org.cloudbus.cloudsim.container.containerProvisioners.CotainerPeProvisionerSimple;
import org.cloudbus.cloudsim.container.containerVmProvisioners.ContainerVmBwProvisionerSimple;
import org.cloudbus.cloudsim.container.containerVmProvisioners.ContainerVmPe;
import org.cloudbus.cloudsim.container.containerVmProvisioners.ContainerVmPeProvisionerSimple;
import org.cloudbus.cloudsim.container.containerVmProvisioners.ContainerVmRamProvisionerSimple;
import org.cloudbus.cloudsim.container.core.*;
import org.cloudbus.cloudsim.container.hostSelectionPolicies.HostSelectionPolicy;
import org.cloudbus.cloudsim.container.hostSelectionPolicies.HostSelectionPolicyFirstFit;
import org.cloudbus.cloudsim.container.lists.ContainerList;
import org.cloudbus.cloudsim.container.lists.ContainerVmList;
import org.cloudbus.cloudsim.container.resourceAllocatorMigrationEnabled.PCVmAllocationPolicyMigrationAbstractHostSelection;
import org.cloudbus.cloudsim.container.resourceAllocators.ContainerVmAllocationPolicy;
import org.cloudbus.cloudsim.container.schedulers.ContainerVmSchedulerTimeSharedOverSubscription;
import org.cloudbus.cloudsim.container.utils.IDs;
import org.cloudbus.cloudsim.container.vmSelectionPolicies.PowerContainerVmSelectionPolicy;
import org.cloudbus.cloudsim.container.vmSelectionPolicies.PowerContainerVmSelectionPolicyMaximumUsage;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
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
    protected List<ServerlessTasks> cloudletList = new ArrayList<ServerlessTasks>();
    private static int overBookingfactor = 0;
    /**
     * The arrival time of each serverless request
     */
    protected Queue<Double> cloudletArrivalTime = new LinkedList<Double>();
    /**
     * The serverless function requests queue
     */
    protected Queue<ServerlessTasks> cloudletQueue = new LinkedList<ServerlessTasks>();
    /**
     * The task type and vm map of controller - contains the list of vms running each function type
     */
    protected Map<String, ArrayList<ServerlessInvoker>> functionVmMap = new HashMap<String, ArrayList<ServerlessInvoker>>();
    protected List<ServerlessTasks> toSubmitOnContainerCreation = new ArrayList<ServerlessTasks>();
    protected List<Double> averageVmUsageRecords = new ArrayList<Double>();
    protected List<Double> meanAverageVmUsageRecords = new ArrayList<Double>();
    protected List<Integer> vmCountList = new ArrayList<Integer>();
    protected List<Double> meanSumOfVmCount = new ArrayList<Double>();
    protected double timeInterval = 50.0;
    protected double cloudletSubmitClock = 0;
    protected Map<ServerlessInvoker, ArrayList<ServerlessTasks>> vmTempTimeMap = new HashMap<ServerlessInvoker,ArrayList<ServerlessTasks>>();
    ServerlessDatacenter e ;

    RequestLoadBalancer loadBalancer;
    public int controllerId=0;
    public int containerId = 1;;
    private boolean reschedule = false;
    int dcount = 1;
    int exsitingContCount = 0;
    public int noOfTasks = 0;
    public int noOfTasksReturned = 0;
    private String vmSelectionMode= "RR";
    private String subMode= "NEWBM";
    /** Vm index for selecting Vm in round robin fashion **/
    private int selectedVmIndex = 1;
    /** The map of tasks to reschedule with the new containerId. */
    private static Map<ServerlessTasks, Integer> tasksToReschedule = new HashMap<>();

    @Override
    protected void processOtherEvent(SimEvent ev){
        switch (ev.getTag()) {
            case CloudSimTags.CLOUDLET_SUBMIT:
//                processCloudletSubmit(ev);
                submitCloudlet(ev);
                break;
            case CloudSimTags.CLOUDLET_SUBMIT_ACK:
                processCloudletSubmitAck(ev);
                break;
            case CloudSimTags.CLOUDLET_RESCHEDULE:
                ((ServerlessTasks) ev.getData()).setReschedule(true);
                submitCloudlet(ev);
                break;
            // other unknown tags are processed by this method
            case CloudSimTags.CONTAINER_DESTROY_ACK:
                processContainerDestroy(ev);
                break;
            case CloudSimTags.SCALED_CONTAINER:
                processScaledContainer(ev);
                break;

            case CloudSimTags.CLOUDLET_MOVE_ACK:
                processCloudletMoveAck(ev);
                break;

            case CloudSimTags.RECORD_CPU_USAGE:
                processRecordCPUUsage(ev);
                break;

//            case CloudSimTags.CREATE_CLOUDLETS:
//                processCreateCloudlets(ev);
//                break;

            default:
                super.processOtherEvent(ev);
                break;
        }
    }

    public ServerlessController(String name, int overBookingfactor) throws Exception {
        super(name, overBookingfactor);
        ServerlessController.overBookingfactor = overBookingfactor;
        createCloudlets();
    }

    @Override
    public void startEntity() {
        super.startEntity();
        while(!cloudletArrivalTime.isEmpty()){
            send(getId(), cloudletArrivalTime.remove(), CloudSimTags.CLOUDLET_SUBMIT,cloudletQueue.remove());

        }
    }

    public void start() throws Exception {
        try {

            controllerId= this.getId();

            e = createDatacenter("datacenter");
            loadBalancer = new RequestLoadBalancer(this, e);

            List<ServerlessInvoker> vmList = createVmList(controllerId);
            this.submitVmList(vmList);

//          the time at which the simulation has to be terminated.
            CloudSim.terminateSimulation(3700.00);

//          Starting the simualtion
            CloudSim.startSimulation();

//          Stopping the simualtion.
            CloudSim.stopSimulation();

//          Printing the results when the simulation is finished.
            List<ContainerCloudlet> newList = this.getCloudletReceivedList();
            printCloudletList(newList);
            if (Constants.monitoring){
                printVmUpDownTime();
                printVmUtilization();
            }

            writeDataLineByLine(newList);


            Log.printLine("ContainerCloudSimExample1 finished!");
        } catch (Exception e) {
            e.printStackTrace();
            Log.printLine("Unwanted errors happen");
        }
    }

    public void createCloudlets() throws IOException {
        long fileSize = 300L;
        long outputSize = 300L;
        int createdCloudlets = 0;
        BufferedReader br = new BufferedReader(new FileReader(Constants.FUNCTION_REQUESTS_FILENAME));
        String line = null;
        String cvsSplitBy = ",";
        noOfTasks++;

        UtilizationModelPartial utilizationModelPar = new UtilizationModelPartial();
        UtilizationModelFull utilizationModel = new UtilizationModelFull();

        while ((line = br.readLine()) != null) {
            String[] data = line.split(cvsSplitBy);
            ServerlessTasks cloudlet = null;

            try {
                cloudlet = new ServerlessTasks(IDs.pollId(ServerlessTasks.class), Double.parseDouble(data[0]), data[1], data[2], Long.parseLong(data[3]), Integer.parseInt(data[4]), Integer.parseInt(data[5]), Double.parseDouble(data[6]), Integer.parseInt(data[7]),
                        fileSize, outputSize, utilizationModelPar, utilizationModel, utilizationModel, false, 0, true);
                System.out.println("Cloudlet No " + cloudlet.getCloudletId());
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(0);
            }

            cloudlet.setUserId(this.getId());
            System.out.println(CloudSim.clock() + " Cloudlet created. This cloudlet arrival time is :" + Double.parseDouble(data[0]));
            cloudletArrivalTime.add(Double.parseDouble(data[0]) + Constants.FUNCTION_SCHEDULING_DELAY);
//            cloudletList.add(cloudlet);
            cloudletQueue.add(cloudlet);
            createdCloudlets += 1;

        }
    }

    public static ServerlessDatacenter createDatacenter(String name) throws Exception {
        String arch = "x86";
        String os = "Linux";
        String vmm = "Xen";
        String logAddress = "~/Results";
        double time_zone = 10.0D;
        double cost = 3.0D;
        double costPerMem = 0.05D;
        double costPerStorage = 0.001D;
        double costPerBw = 0.0D;

        List<ContainerHost> hostList = createHostList(Constants.NUMBER_HOSTS);
        //        Select hosts to migrate
        HostSelectionPolicy hostSelectionPolicy = new HostSelectionPolicyFirstFit();
        //        Select vms to migrate
        PowerContainerVmSelectionPolicy vmSelectionPolicy = new PowerContainerVmSelectionPolicyMaximumUsage();
        //       Allocating host to vm
        ContainerVmAllocationPolicy vmAllocationPolicy = new
                PCVmAllocationPolicyMigrationAbstractHostSelection(hostList, vmSelectionPolicy,
                hostSelectionPolicy, Constants.overUtilizationThreshold, Constants.underUtilizationThreshold);
        //      Allocating vms to container
        FunctionScheduler containerAllocationPolicy = new FunctionScheduler();
        //  Load Balancer for routing function requests


        ContainerDatacenterCharacteristics characteristics = new
                ContainerDatacenterCharacteristics(arch, os, vmm, hostList, time_zone, cost, costPerMem, costPerStorage,
                costPerBw);
        /** Set datacenter monitoring to true if metrics monitoring is required **/
        ServerlessDatacenter datacenter = new ServerlessDatacenter(name, characteristics, vmAllocationPolicy,
                containerAllocationPolicy, new LinkedList<Storage>(), Constants.SCHEDULING_INTERVAL, getExperimentName("SimTest1", String.valueOf(overBookingfactor)), logAddress,
                Constants.VM_STARTTUP_DELAY, Constants.CONTAINER_STARTTUP_DELAY, Constants.monitoring);

        return datacenter;
    }

    private static String getExperimentName(String... args) {
        StringBuilder experimentName = new StringBuilder();

        for (int i = 0; i < args.length; ++i) {
            if (!args[i].isEmpty()) {
                if (i != 0) {
                    experimentName.append("_");
                }

                experimentName.append(args[i]);
            }
        }

        return experimentName.toString();
    }

    public static List<ContainerHost> createHostList(int hostsNumber) {
        ArrayList<ContainerHost> hostList = new ArrayList<ContainerHost>();
        for (int i = 0; i < hostsNumber; ++i) {
            int hostType = i / (int) Math.ceil((double) hostsNumber / 3.0D);
            // System.out.println("Host type is: "+ hostType);
            ArrayList<ContainerVmPe> peList = new ArrayList<ContainerVmPe>();
            for (int j = 0; j < Constants.HOST_PES[hostType]; ++j) {
                peList.add(new ContainerVmPe(j,
                        new ContainerVmPeProvisionerSimple((double) Constants.HOST_MIPS[hostType])));
            }

            hostList.add(new PowerContainerHostUtilizationHistory(IDs.pollId(ContainerHost.class),
                    new ContainerVmRamProvisionerSimple(Constants.HOST_RAM[hostType]),
                    new ContainerVmBwProvisionerSimple(1000000L), 1000000L, peList,
                    new ContainerVmSchedulerTimeSharedOverSubscription(peList),
                    Constants.HOST_POWER[hostType]));
        }

        return hostList;
    }

    private static ArrayList<ServerlessInvoker> createVmList(int brokerId) {
        ArrayList<ServerlessInvoker> containerVms = new ArrayList<ServerlessInvoker>();
        for (int i = 0; i < Constants.NUMBER_VMS; ++i) {
            ArrayList<ContainerPe> peList = new ArrayList<ContainerPe>();
            Random rand = new Random();
            int vmType = rand.nextInt(4);
            for (int j = 0; j < Constants.VM_PES[vmType]; ++j) {
                peList.add(new ContainerPe(j,
                        new CotainerPeProvisionerSimple((double) Constants.VM_MIPS[vmType])));
            }
            containerVms.add(new ServerlessInvoker(IDs.pollId(ContainerVm.class), brokerId,
                    (double) Constants.VM_MIPS[vmType], (float) Constants.VM_RAM[vmType],
                    Constants.VM_BW, Constants.VM_SIZE, "Xen",
                    new ServerlessContainerScheduler(peList),
                    new ServerlessContainerRamProvisioner(Constants.VM_RAM[vmType]),
                    new ContainerBwProvisionerSimple(Constants.VM_BW),
                    peList, Constants.SCHEDULING_INTERVAL));


        }

        return containerVms;
    }

    public void submitCloudlet(SimEvent ev) {
        ServerlessTasks cl = (ServerlessTasks) ev.getData();
        System.out.println(CloudSim.clock() + " Cloudlet arrived: " + cl.getCloudletId());
        if (CloudSim.clock() == cloudletSubmitClock) {
            send(getId(), Constants.MINIMUM_INTERVAL_BETWEEN_TWO_CLOUDLET_SUBMISSIONS, CloudSimTags.CLOUDLET_SUBMIT, cl);
        }
        else {
            e.updateCloudletProcessing();
            e.destroyIdleContainers();

            if (cl.getReschedule()) {
                removeFromVmTaskMap(cl, (ServerlessInvoker) (ContainerVmList.getById(getVmsCreatedList(), cl.getVmId())));
                removeFromVmTaskExecutionMap(cl, (ServerlessInvoker) (ContainerVmList.getById(getVmsCreatedList(), cl.getVmId())));
            }
            else {
                submitCloudletoList(cl);
            }
            loadBalancer.routeRequest(cl);
/*********************************Serverless Architecture 1********************************/
//            if (!Constants.containerConcurrency && (cl.getVmId() == -1 || cl.getReschedule())) {
//                loadBalancer.routeRequest(cl);
////                ServerlessInvoker vm = selectVM(cl);
////                if (vm != null) {
//////                    cl.setContainerId(containerId);
////                    toSubmitOnContainerCreation.add(cl);
//////                    addToVmTaskMap(cl, vm);
////                    createContainer(cl, cl.getcloudletFunctionId(), cl.getUserId(), vm);
////                } else {
////                    System.out.println("Cloudlet #" + cl.getCloudletId() + " has no vm to execute");
////                }
//                cloudletSubmitClock = CloudSim.clock();
//
//            }
///*********************************Serverless Architecture 2********************************/
//            else if (Constants.containerConcurrency && (cl.getContainerId() == -1 || cl.getReschedule())) {
//                if (cl.retry > Constants.max_reschedule_tries){
//                    cl.setSuccess(false);
//                    getCloudletReceivedList().add(cl);
//                }
//                else{
//                    boolean containerSelected = selectContainer(cl);
//                    if (!containerSelected) {
//                        getCloudletList().remove(cl);
//                    }
//                }
//
//
//
//            }
        }

    }

    protected void sendFunctionRetryRequest(ServerlessTasks req){
        send(getId(), Constants.FUNCTION_SCHEDULING_RETRY_DELAY, CloudSimTags.CLOUDLET_SUBMIT, req);
    }



    protected void createContainer(ServerlessTasks cl, String cloudletId, int brokerId) {
        double containerMips = 0;
/**     container MIPS is set as specified for that container type  **/
        containerMips = Constants.CONTAINER_MIPS[Integer.parseInt(cloudletId)];
        ServerlessContainer container = new ServerlessContainer(containerId, brokerId, cloudletId, containerMips, Constants.CLOUDLET_PES, Constants.CONTAINER_RAM[Integer.parseInt(cloudletId)], Constants.CONTAINER_BW, Constants.CONTAINER_SIZE,"Xen", new ServerlessCloudletScheduler(containerMips, Constants.CONTAINER_PES[Integer.parseInt(cloudletId)]), Constants.SCHEDULING_INTERVAL, true, false);
        getContainerList().add(container);
        if (!(cl ==null)){
            cl.setContainerId(containerId);
        }
//        if (!Constants.containerConcurrency){
//            container.setVm(vm);
//        }
        submitContainer(cl, container);
        containerId++;


    }
    protected void processScaledContainer(SimEvent ev){
        int[] data = (int[]) ev.getData();
        int brokerId = data[0];
        String cloudletId = String.valueOf((data[1]));
        double containerMips = Constants.CONTAINER_MIPS[Integer.parseInt(cloudletId)];
        ServerlessContainer container = new ServerlessContainer(containerId, brokerId, cloudletId, containerMips, Constants.CLOUDLET_PES, Constants.CONTAINER_RAM[Integer.parseInt(cloudletId)], Constants.CONTAINER_BW, Constants.CONTAINER_SIZE,"Xen", new ServerlessCloudletScheduler(containerMips, Constants.CONTAINER_PES[Integer.parseInt(cloudletId)]), Constants.SCHEDULING_INTERVAL, true, false);
        getContainerList().add(container);
        container.setWorkloadMips(container.getMips());
        sendNow(getDatacenterIdsList().get(0), containerCloudSimTags.CONTAINER_SUBMIT, container);
        containerId++;

    }

    protected void submitContainer(ServerlessTasks cl, Container container){
        container.setWorkloadMips(container.getMips());
        if(cl.getReschedule()){
            sendNow(getDatacenterIdsList().get(0), containerCloudSimTags.CONTAINER_SUBMIT_FOR_RESCHEDULE, container);
        }
        else
            sendNow(getDatacenterIdsList().get(0), containerCloudSimTags.CONTAINER_SUBMIT, container);

    }

    protected void addToVmTaskMap(ServerlessTasks task, ServerlessInvoker vm){
        int count = vm.getvmTaskMap().getOrDefault(task.getcloudletFunctionId(), 0);
        vm.getvmTaskMap().put(task.getcloudletFunctionId(), count+1);

    }
    public void removeFromVmTaskMap(ServerlessTasks task, ServerlessInvoker vm){
        //System.out.println("Trying to remove from map task # "+task.getCloudletId()+"Now task map of VM: "+vm.getId()+" "+ vm.getvmTaskMap());
        int count = vm.getvmTaskMap().get(task.getcloudletFunctionId());
        vm.getvmTaskMap().put(task.getcloudletFunctionId(), count-1);
        if(count==1){
            functionVmMap.get(task.getcloudletFunctionId()).remove(vm);
            if(functionVmMap.get(task.getcloudletFunctionId())==null){
                functionVmMap.remove(task.getcloudletFunctionId());
            }
        }
    }

    public void removeFromVmTaskExecutionMap(ServerlessTasks task, ServerlessInvoker vm){
        vm.getVmTaskExecutionMap().get(task.getcloudletFunctionId()).remove(task);
        vm.getVmTaskExecutionMapFull().get(task.getcloudletFunctionId()).remove(task);
    }

    public void submitCloudletoList(ServerlessTasks cloudlet) {
        getCloudletList().add(cloudlet);
    }

    /**
     * Insert the policy for selecting a VM when container concurrency is not enableld
     */
    public ServerlessInvoker selectVM(ServerlessTasks cloudlet) {
        ServerlessInvoker selectedVm = null;
        boolean vmSelected = false;
        switch (Constants.vmSelectionAlgo) {
            /** Selecting Vm using RR method **/
            case "RR":
                for (int x = selectedVmIndex; x <= vmsCreatedList.size(); x++) {
                    ServerlessInvoker tempSelectedVm = (ServerlessInvoker) (ContainerVmList.getById(getVmsCreatedList(), x));
                    if (cloudlet.getReschedule()) {
                        if (tempSelectedVm == (ServerlessInvoker) (ContainerVmList.getById(getVmsCreatedList(), cloudlet.getVmId()))) {
                            continue;
                        }
                    }
                    double vmCPUUsage = 1 - tempSelectedVm.getAvailableMips() / tempSelectedVm.getTotalMips();

                    if (cloudlet.getcloudletMemory() <= tempSelectedVm.getContainerRamProvisioner().getAvailableVmRam() && Constants.CONTAINER_BW <= tempSelectedVm.getContainerBwProvisioner().getAvailableVmBw() && Constants.CONTAINER_SIZE <= tempSelectedVm.getSize() ) {
                        selectedVm = tempSelectedVm;
                        vmSelected = true;
                        break;
                    }
                }
                if (vmSelected == false) {
                    for (int x = 1; x < selectedVmIndex; x++) {
                        ServerlessInvoker tempSelectedVm = (ServerlessInvoker) (ContainerVmList.getById(getVmsCreatedList(), x));
                        if (cloudlet.getReschedule()) {
                            if (tempSelectedVm == (ServerlessInvoker) (ContainerVmList.getById(getVmsCreatedList(), cloudlet.getVmId()))) {
                                continue;
                            }
                        }
                        double vmCPUUsage = 1 - tempSelectedVm.getAvailableMips() / tempSelectedVm.getTotalMips();
                        if (cloudlet.getcloudletMemory() <= tempSelectedVm.getContainerRamProvisioner().getAvailableVmRam() && Constants.CONTAINER_BW <= tempSelectedVm.getContainerBwProvisioner().getAvailableVmBw() && Constants.CONTAINER_SIZE <= tempSelectedVm.getSize() ) {
                            selectedVm = tempSelectedVm;
                            vmSelected = true;
                            break;
                        }
                    }
                }
                System.out.println(CloudSim.clock() + " >>>>>>>>>>>>>>Debug:Broker: Selected VM is # " + selectedVmIndex + " for cloudlet # " + cloudlet.getCloudletId() + " under RR method");


//                if (reschedule == false) {
//                    cloudlet.setContainerId(containerId);
//                    toSubmitOnContainerCreation.add(cloudlet);
//                }
                if (cloudlet.getReschedule()) {
                    tasksToReschedule.put(cloudlet, containerId);
                    // System.out.println(CloudSim.clock() + " Debug:Broker: Need to create a new container# " + containerId + " to reschedule cloudlet # " + cl.getCloudletId());

                }

                if (selectedVmIndex == vmsCreatedList.size()) {
                    selectedVmIndex = 1;
                } else
                    selectedVmIndex++;

            /** Selecting Vm using Random method **/
            case "RM":
                Random random = new Random();
                while(vmSelected == false) {
                    int vmNo = random.nextInt(vmsCreatedList.size() - 1);
                    ServerlessInvoker tempSelectedVm = (ServerlessInvoker) (ContainerVmList.getById(getVmsCreatedList(), vmNo));
                    if (cloudlet.getReschedule()) {
                        if (tempSelectedVm == (ServerlessInvoker) (ContainerVmList.getById(getVmsCreatedList(), cloudlet.getVmId()))) {
                            continue;
                        }
                    }

                    double vmCPUUsage = 1 - tempSelectedVm.getAvailableMips() / tempSelectedVm.getTotalMips();
                    if (cloudlet.getcloudletMemory() <= tempSelectedVm.getContainerRamProvisioner().getAvailableVmRam() && Constants.CONTAINER_BW <= tempSelectedVm.getContainerBwProvisioner().getAvailableVmBw() && Constants.CONTAINER_SIZE <= tempSelectedVm.getSize() ) {
                        System.out.println(CloudSim.clock() + " >>>>>>>>>>>>>>Debug:Broker: Vm # " + tempSelectedVm.getId() + " has available ram of " + tempSelectedVm.getContainerRamProvisioner().getAvailableVmRam());
                        selectedVm = tempSelectedVm;
                        vmSelected = true;
                    }
                }

                System.out.println(CloudSim.clock() + " >>>>>>>>>>>>>>Debug:Broker: Selected VM is # " + selectedVmIndex + " for cloudlet # " + cloudlet.getCloudletId() + " under RR method");

                if (cloudlet.getReschedule()) {
                    tasksToReschedule.put(cloudlet, containerId);
                    // System.out.println(CloudSim.clock() + " Debug:Broker: Need to create a new container# " + containerId + " to reschedule cloudlet # " + cl.getCloudletId());

                }
            }

        return selectedVm;

    }

    @Override
    /*process cloudlets to submit after container creation*/
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
                getContainersCreatedList().add(cont);
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
        List<ServerlessTasks> toRemove = new ArrayList<>();
        if (!toSubmitOnContainerCreation.isEmpty()) {
            for(ServerlessTasks cloudlet: toSubmitOnContainerCreation){
                if(cloudlet.getContainerId()==containerId) {
                    ServerlessInvoker vm = (ServerlessInvoker)(ContainerVmList.getById(getVmsCreatedList(),vmId));
                    if(vm!=null) {
                        addToVmTaskMap(cloudlet, vm);
                        vmTempTimeMap.get(vm).remove(cloudlet);
                        vm.setFunctionContainerMap(cont, cloudlet.getcloudletFunctionId());
                        setFunctionVmMap(((ServerlessInvoker) (ContainerVmList.getById(getVmsCreatedList(), vmId))), cloudlet.getcloudletFunctionId());
                        submitCloudletToDC(cloudlet, vmId, 0, containerId);
                        getContainerList().remove(ContainerList.getById(getContainerList(), containerId));
                        toRemove.add(cloudlet);
                    }
                }
            }
            //Log.print(getContainersCreatedList().size() + "vs asli"+getContainerList().size());

            toSubmitOnContainerCreation.removeAll(toRemove);
            toRemove.clear();
        }

        dcount++;

        if(!tasksToReschedule.isEmpty()){
            Map<ServerlessTasks, Integer> removeFromMap = new HashMap<>();
//            reschedule=true;

            for (Map.Entry<ServerlessTasks, Integer> entry : tasksToReschedule.entrySet()) {
                if(entry.getValue()==containerId){
                    ServerlessInvoker vm=((ServerlessInvoker)(ContainerVmList.getById(getVmsCreatedList(),vmId)));
                    if(vm!=null) {
                        vm.setFunctionContainerMap(ContainerList.getById(getContainerList(), containerId), entry.getKey().getcloudletFunctionId());
                        setFunctionVmMap(((ServerlessInvoker) (ContainerVmList.getById(getVmsCreatedList(), vmId))), entry.getKey().getcloudletFunctionId());
                        submitCloudletToDC(entry.getKey(), vmId, 0, containerId);
                        getContainerList().remove(ContainerList.getById(getContainerList(), containerId));
                        removeFromMap.put(entry.getKey(), entry.getValue());
                    }

                }
            }
            tasksToReschedule.remove(removeFromMap);
            removeFromMap.clear();

        }
//        reschedule=false;

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
            ArrayList<ServerlessTasks> taskList = new ArrayList<>();
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
//        if (getVmsCreatedList().size() == getVmList().size() - getVmsDestroyed()) {
//        If we have tried creating all of the vms in the data center, we submit the containers.
        /*if (getVmList().size() == vmsAcks) {

            submitContainers();
        }*/
    }

    public void submitCloudletToDC(ServerlessTasks cloudlet, int vmId, double delay, int containerId){
        if(!cloudlet.getReschedule()) {
            cloudlet.setVmId(vmId);
            cloudletsSubmitted++;
            getCloudletSubmittedList().add(cloudlet);
            getCloudletList().remove(cloudlet);
            //System.out.println("Time " + cloudlet.getMaxExecTime());


            Log.print(String.format("%f: Cloudlet %d has been submitted to VM %d and container %d", CloudSim.clock(), cloudlet.getCloudletId(), vmId, containerId, " "));
            if (delay > 0) {
                send(getVmsToDatacentersMap().get(cloudlet.getVmId()), delay, CloudSimTags.CLOUDLET_SUBMIT_ACK, cloudlet);
            } else
                sendNow(getVmsToDatacentersMap().get(cloudlet.getVmId()), CloudSimTags.CLOUDLET_SUBMIT_ACK, cloudlet);
        }

        else{
            int[] data = new int[7];
            data[0] = cloudlet.getCloudletId();
            data[1] = cloudlet.getUserId();
            data[2] = cloudlet.getVmId();
            data[3] = cloudlet.getContainerId();
            data[4] = vmId;
            data[5] = containerId;
            data[6] = getVmsToDatacentersMap().get(vmId);

            //System.out.println("Container lsit size "+ ContainerVmList.getById(getVmsCreatedList(),vmId).getContainerList().size());
            Log.print(String.format("%f: Cloudlet %d to reschedule has been submitted to VM %d container %d ", CloudSim.clock(), cloudlet.getCloudletId(), vmId, containerId));

            sendNow(getVmsToDatacentersMap().get(vmId),CloudSimTags.CLOUDLET_MOVE_ACK,data);
        }

    }

    public void processCloudletSubmitAck (SimEvent ev){
        ServerlessTasks task = (ServerlessTasks) ev.getData();
        Container cont = ContainerList.getById(getContainersCreatedList(), task.getContainerId());
        ServerlessInvoker vm = ContainerVmList.getById(getVmsCreatedList(),task.getVmId());
        ((ServerlessContainer)cont).newContainer =false;
    }

    public void processCloudletMoveAck(SimEvent ev) {
        Pair data = (Pair) ev.getData();
        ServerlessInvoker oldVm = ContainerVmList.getById(getVmsCreatedList(), (Integer) data.getValue());
        ServerlessInvoker newVm = ContainerVmList.getById(getVmsCreatedList(), ((ServerlessTasks) data.getKey()).getVmId());
        Container cont = ContainerList.getById(getContainersCreatedList(), ((ServerlessTasks) data.getKey()).getContainerId());
        ((ServerlessContainer) cont).newContainer = false;

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
            setContainersCreated(getContainersCreated()-1);
        }else{
            Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Failed to destroy Container #", containerId);
        }

    }

    @Override

    protected void processCloudletReturn(SimEvent ev) {
        Pair data = (Pair) ev.getData();
        ContainerCloudlet cloudlet = (ContainerCloudlet) data.getKey();
        ContainerVm vm = (ContainerVm) data.getValue();


        removeFromVmTaskMap((ServerlessTasks)cloudlet,(ServerlessInvoker)vm);
        removeFromVmTaskExecutionMap((ServerlessTasks)cloudlet,(ServerlessInvoker)vm);

        getCloudletReceivedList().add(cloudlet);
        Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Cloudlet ", cloudlet.getCloudletId(),
                " returned");
        //Log.printConcatLine(CloudSim.clock(), ": ", getName(), "The number of finished Cloudlets is:", getCloudletReceivedList().size());
        /**Debugger */
        //System.out.println(CloudSim.clock()+" Debugger: CLoudletlist size: "+getCloudletList().size()+" Cloudletssubmitted: "+cloudletsSubmitted );
        cloudletsSubmitted--;


        noOfTasksReturned++;
        /*if(noOfTasksReturned==Constants.NUM_TASKS){
            for(int x=0; x<getVmsCreatedList().size(); x++){
                ServerlessInvoker invoker = ((ServerlessInvoker)getVmsCreatedList().get(x));
                if(invoker.inTime< invoker.outTime){
                    double outTimeRecorded = (invoker.getVmUpTime()).get("Out");
                    (invoker.getVmUpTime()).put("Out",outTimeRecorded+CloudSim.clock()-invoker.outTime);

                }
                else{
                    double inTimeRecorded = (invoker.getVmUpTime()).get("In");
                    (invoker.getVmUpTime()).put("In",inTimeRecorded+CloudSim.clock()-invoker.inTime);
                }

            }
        }*/



//        if (getCloudletList().size() == 0 && cloudletsSubmitted == 0) { // all cloudlets executed
//            Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": All Cloudlets executed. Finishing...");
//            clearDatacenters();
//            finishExecution();
//        }
/*        else { // some cloudlets haven't finished yet
            if (getCloudletList().size() > 0 && cloudletsSubmitted == 0) {
                // all the cloudlets sent finished. It means that some bount
                // cloudlet is waiting its VM be created
                clearDatacenters();
                createVmsInDatacenter(0);
            }

        }*/
    }




    private static void printCloudletList(List<ContainerCloudlet> list) {
        int size = list.size();
        Cloudlet cloudlet;
        int deadlineMetStat = 0;

        String indent = "    ";
        Log.printLine();
        Log.printLine("========== OUTPUT ==========");
        Log.printLine("Cloudlet ID" + indent + "STATUS" + indent
                + "Data center ID" + indent + "Final VM ID" + indent + "Execution Time" + indent
                + "Start Time" + indent + "Finish Time"+ indent + "Response Time"+ indent + "Vm List");

        DecimalFormat dft = new DecimalFormat("###.##");
        for (int i = 0; i < size; i++) {
            cloudlet = list.get(i);
            Log.print(indent + cloudlet.getCloudletId() + indent + indent);

            if (cloudlet.getCloudletStatusString() == "Success") {
                Log.print("SUCCESS");
                if (Math.ceil((cloudlet.getFinishTime() - ((ServerlessTasks) cloudlet).getArrivalTime())) <= (Math.ceil(((ServerlessTasks) cloudlet).getMaxExecTime())) || Math.floor((cloudlet.getFinishTime() - ((ServerlessTasks) cloudlet).getArrivalTime())) <= (Math.ceil(((ServerlessTasks) cloudlet).getMaxExecTime()))) {
                    deadlineMetStat++;
                }
            }
            else{
                Log.print("DROPPED");
            }

            Log.printLine(indent + indent + cloudlet.getResourceId()
                    + indent + indent + indent +indent + cloudlet.getVmId()
                    + indent + indent+ indent+ indent
                    + dft.format(cloudlet.getActualCPUTime()) + indent+ indent
                    + indent + dft.format(cloudlet.getExecStartTime())
                    + indent + indent+ indent
                    + dft.format(cloudlet.getFinishTime())+ indent + indent + indent
                    + dft.format(cloudlet.getFinishTime()-((ServerlessTasks)cloudlet).getArrivalTime())+ indent + indent + indent
                    + ((ServerlessTasks)cloudlet).getResList());


        }

        Log.printLine("Deadline met no: "+deadlineMetStat);



    }

    private void printVmUtilization(){
//        for(int x=0; x<getVmsCreatedList().size(); x++){
        System.out.println("Average CPU utilization of vms: "+ getAverageResourceUtilization());
        System.out.println("Average vm count: "+ getAverageVmCount());
        System.out.println("Using exsiting cont: "+ exsitingContCount);
            /*System.out.println("CPU usage records "+ ((ServerlessInvoker)getVmsCreatedList().get(x)).getCPUUsageRecords());
            System.out.println("Average CPU records "+ ((ServerlessInvoker)getVmsCreatedList().get(x)).getAverageCPUUsageRecords());*/

//        }
    }

    public void processRecordCPUUsage(SimEvent ev){
        /*ServerlessInvoker vm = (ServerlessInvoker)ev.getData();
        double sum = 0;
        double utilization   = 1 - vm.getAvailableMips() / vm.getTotalMips();
        if(utilization>0) {
            vm.getCPUUsageRecords().add(utilization);
            if(vm.getId()==2){
                System.out.println(" CPU record fo vm 2 "+utilization);
            }
            if (vm.getCPUUsageRecords().size() == Constants.CPU_HISTORY_LENGTH) {
                for (int x = 0; x < vm.getCPUUsageRecords().size(); x++) {
                    sum += vm.getCPUUsageRecords().get(x);
                }
                vm.getAverageCPUUsageRecords().add(sum / Constants.CPU_HISTORY_LENGTH);

                vm.getCPUUsageRecords().clear();
            }

        }*/

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


        send(this.getId(), Constants.CPU_USAGE_MONITORING_INTERVAL, CloudSimTags.RECORD_CPU_USAGE);


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





    private  void printVmUpDownTime(){
        double totalVmUpTime = 0;

        for(int x=0; x<getVmsCreatedList().size(); x++){
            ServerlessInvoker vm = ((ServerlessInvoker)getVmsCreatedList().get(x));
            /*System.out.println("on time "+vm.onTime);
            System.out.println("off time "+vm.offTime);
            System.out.println("status "+vm.getStatus());
            System.out.println("Record time"+vm.getRecordTime());
            System.out.println("time "+CloudSim.clock());*/
            if(vm.getStatus().equals("OFF")){
                vm.offTime += 3700.00-vm.getRecordTime();
            }
            else if(vm.getStatus().equals("ON")){
                vm.onTime += 3700.00-vm.getRecordTime();
            }
            System.out.println("Vm # "+getVmsCreatedList().get(x).getId()+" used: "+vm.used );
            System.out.println("Vm # "+getVmsCreatedList().get(x).getId()+"has uptime of: "+vm.onTime);
            System.out.println("Vm # "+getVmsCreatedList().get(x).getId()+"has downtime of: "+vm.offTime);
            totalVmUpTime +=vm.onTime;
//            System.out.println("Vm # "+getVmsCreatedList().get(x).getId()+"has time map of : "+((ServerlessInvoker)getVmsCreatedList().get(x)).getVmUpTime());
        }
    }



    private  void writeDataLineByLine(List<ContainerCloudlet> list) throws ParameterException {
        // first create file object for file placed at location
        // specified by filepath
        int size = list.size();
        int successfulRequestCount = 0;
        int droppedRequestCount = 0;
        Cloudlet cloudlet;
        String indent = "    ";
        java.io.File file = new java.io.File("D:\\UniMelb\\Studying\\CloudSim\\data\\OW_Output.csv");

        try {
            // create FileWriter object with file as parameter
            FileWriter outputfile = new FileWriter(file);

            // create CSVWriter object filewriter object as parameter
            CSVWriter writer = new CSVWriter(outputfile);

            // adding header to csv
            String[] header = { "Cloudlet ID", "STATUS", "Data center ID","Final VM ID" ,"Execution Time" ,"Start Time" ,"Finish Time","Response Time","Vm List", "Max execution time" , "Priority"};
            writer.writeNext(header);

            for (ContainerCloudlet containerCloudlet : list) {
                cloudlet = containerCloudlet;

                if (cloudlet.getCloudletStatusString().equals("Success")) {
                    successfulRequestCount++;
                    String[] data = {String.valueOf(cloudlet.getCloudletId()), "SUCCESS", String.valueOf(cloudlet.getResourceId()), String.valueOf(cloudlet.getVmId()), String.valueOf(cloudlet.getActualCPUTime()), String.valueOf(cloudlet.getExecStartTime()), String.valueOf(cloudlet.getFinishTime()), String.valueOf((cloudlet.getFinishTime() - ((ServerlessTasks) cloudlet).getArrivalTime())), ((ServerlessTasks) cloudlet).getResList(), String.valueOf(((ServerlessTasks) cloudlet).getMaxExecTime()), String.valueOf(((ServerlessTasks) cloudlet).getPriority())};
                    writer.writeNext(data);

                } else {
                    droppedRequestCount++;
                    String[] data = {String.valueOf(cloudlet.getCloudletId()), "DROPPED", String.valueOf(cloudlet.getResourceId()), String.valueOf(cloudlet.getVmId()), String.valueOf(cloudlet.getActualCPUTime()), String.valueOf(cloudlet.getExecStartTime()), String.valueOf(cloudlet.getFinishTime()), String.valueOf((cloudlet.getFinishTime() - ((ServerlessTasks) cloudlet).getArrivalTime())), ((ServerlessTasks) cloudlet).getResList(), String.valueOf(((ServerlessTasks) cloudlet).getMaxExecTime()), String.valueOf(((ServerlessTasks) cloudlet).getPriority())};
                    writer.writeNext(data);
                }
            }

            if (Constants.monitoring){
                for(int x=0; x<getVmsCreatedList().size(); x++){
                    String[] data = {"Vm # ", String.valueOf(getVmsCreatedList().get(x).getId()),String.valueOf(((ServerlessInvoker)getVmsCreatedList().get(x)).onTime),String.valueOf(((ServerlessInvoker)getVmsCreatedList().get(x)).offTime)};
                    writer.writeNext(data);
                }
            }

            String[] data1 = {"Successful Request Count # ", String.valueOf(successfulRequestCount)};
            writer.writeNext(data1);
            String[] data2 = {"Dropped Request Count # ", String.valueOf(droppedRequestCount)};
            writer.writeNext(data2);



            // closing writer connection
            writer.close();
        }
        catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
