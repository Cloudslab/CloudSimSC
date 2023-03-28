package org.cloudbus.cloudsim.serverless;

import com.opencsv.CSVWriter;
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
import org.cloudbus.cloudsim.container.lists.ContainerVmList;
import org.cloudbus.cloudsim.container.resourceAllocatorMigrationEnabled.PCVmAllocationPolicyMigrationAbstractHostSelection;
import org.cloudbus.cloudsim.container.resourceAllocators.ContainerAllocationPolicy;
import org.cloudbus.cloudsim.container.resourceAllocators.ContainerVmAllocationPolicy;
import org.cloudbus.cloudsim.container.schedulers.ContainerVmSchedulerTimeSharedOverSubscription;
import org.cloudbus.cloudsim.container.utils.IDs;
import org.cloudbus.cloudsim.container.vmSelectionPolicies.PowerContainerVmSelectionPolicy;
import org.cloudbus.cloudsim.container.vmSelectionPolicies.PowerContainerVmSelectionPolicyMaximumUsage;
import org.cloudbus.cloudsim.container.resourceAllocatorMigrationEnabled;
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

    private static List<ServerlessInvoker> vmList = new ArrayList<ServerlessInvoker>();
    private static List<ServerlessInvoker> vmIdleList = new ArrayList<>();
    private static List<Container> containerList = new ArrayList<Container>();
    private static List<ServerlessTasks> cloudletList = new ArrayList<ServerlessTasks>();
    private static int overBookingfactor;
    private Queue<Double> cloudletArrivalTime = new LinkedList<Double>();
    private Map<String, ArrayList<ServerlessInvoker>> functionVmMap = new HashMap<String, ArrayList<ServerlessInvoker>>();
    private Queue cloudletQueue = new LinkedList<ServerlessTasks>();
    private List<ServerlessTasks> toSubmitOnContainerCreation = new ArrayList<ServerlessTasks>();
    private List<Double> averageVmUsageRecords = new ArrayList<Double>();
    private List<Double> meanAverageVmUsageRecords = new ArrayList<Double>();
    private List<Integer> vmCountList = new ArrayList<Integer>();
    private List<Double> meanSumOfVmCount = new ArrayList<Double>();
    private double timeInterval = 50.0;
    private double cloudletSubmitClock = 0;
    private Map<ServerlessInvoker, ArrayList<ServerlessTasks>> vmTempTimeMap = new HashMap<ServerlessInvoker,ArrayList<ServerlessTasks>>();
    private ServerlessDatacenter e ;
    public int controllerId=0;
    public static int containerId = 1;
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
                reschedule = true;
                submitCloudlet(ev);
                break;
            // other unknown tags are processed by this method
            case CloudSimTags.CONTAINER_DESTROY_ACK:
                processContainerDestroy(ev);
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

    public ServerlessController(String name, double overBookingfactor) throws Exception {
        super(name, overBookingfactor);
        ServerlessController.overBookingfactor = (int) overBookingfactor;
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

            vmList = createVmList(controllerId);
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
            printVmUpDownTime();
            printVmUtilization();
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

        UtilizationModelFull utilizationModel = new UtilizationModelFull();

        while ((line = br.readLine()) != null) {
            String[] data = line.split(cvsSplitBy);
            /*System.out.println(">>>> " + data.length);
            System.out.println(data[0]+" "+data[1]+" "+data[2]+" "+data[3]+" "+data[4]);*/
            ServerlessTasks cloudlet = null;

            try {
                cloudlet = new ServerlessTasks(IDs.pollId(ServerlessTasks.class), Double.parseDouble(data[0]), data[1], data[2], Long.parseLong(data[3]), Integer.parseInt(data[4]), Integer.parseInt(data[5]), Double.parseDouble(data[6]), Integer.parseInt(data[7]),
                        fileSize, outputSize, utilizationModel, utilizationModel, utilizationModel);
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
        //        Host to migrate
        HostSelectionPolicy hostSelectionPolicy = new HostSelectionPolicyFirstFit();
        //        Select vms to migrate
        PowerContainerVmSelectionPolicy vmSelectionPolicy = new PowerContainerVmSelectionPolicyMaximumUsage();
        //       Allocating host to vm
        ContainerVmAllocationPolicy vmAllocationPolicy = new
                PCVmAllocationPolicyMigrationAbstractHostSelection(hostList, vmSelectionPolicy,
                hostSelectionPolicy, Constants.overUtilizationThreshold, Constants.underUtilizationThreshold);
        //      Allocating vms to container
        ContainerAllocationPolicy containerAllocationPolicy = new ServerlessContainerAllocationPolicy();

        ContainerDatacenterCharacteristics characteristics = new
                ContainerDatacenterCharacteristics(arch, os, vmm, hostList, time_zone, cost, costPerMem, costPerStorage,
                costPerBw);
        ServerlessDatacenter datacenter = new ServerlessDatacenter(name, characteristics, vmAllocationPolicy,
                containerAllocationPolicy, new LinkedList<Storage>(), Constants.SCHEDULING_INTERVAL, getExperimentName("SimTest1", String.valueOf(overBookingfactor)), logAddress,
                Constants.VM_STARTTUP_DELAY, Constants.CONTAINER_STARTTUP_DELAY);

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
//            int vmType = i / (int) Math.ceil((double) containerVmsNumber / 4.0D);
            Random rand = new Random();
            int vmType = rand.nextInt(4);
            // System.out.println("Vm type is: "+ vmType);
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
            submitCloudletoList(cl);
            if (!Constants.container_concurrency && cl.getVmId() == -1) {
                ServerlessInvoker vm = selectVM(cl);
                if (vm != null) {
                    cl.setContainerId(containerId);
                    toSubmitOnContainerCreation.add(cl);
                    addToVmTaskMap(cl, vm);
                    createContainer(cl.getUserId(), getDatacenterIdsList().get(0), (vm.getHost()).getId(), vm.getId(), vm, containerId, cl.getcloudletMemory());
                    containerId++;
                } else {
                    System.out.println("Cloudlet #" + cl.getCloudletId() + " has no vm to execute");
                }
                cloudletSubmitClock = CloudSim.clock();

            }
        }

    }

    protected void createContainer(int brokerId, int dcId, int hostId, int vmId, ServerlessInvoker vm, int containerId, int memory) {
        double containerMips = 0;
//      container MIPS is set in proportion to the ram allocation
        containerMips = vm.getTotalMips()*(memory/(vm.getRam()));
        ServerlessContainer container = new ServerlessContainer(containerId, brokerId, containerMips, Constants.CLOUDLET_PES, memory, Constants.CONTAINER_BW, Constants.CONTAINER_SIZE,"Xen", new ServerlessCloudletScheduler(containerMips, Constants.CLOUDLET_PES), Constants.SCHEDULING_INTERVAL, true);
        getContainerList().add(container);
        container.setVm(vm);
        submitContainer(container);

    }

    protected void submitContainer(Container container){
        container.setWorkloadMips(container.getMips());
        sendNow(getDatacenterIdsList().get(0), containerCloudSimTags.CONTAINER_SUBMIT, container);

    }

    protected void addToVmTaskMap(ServerlessTasks task, ServerlessInvoker vm){
        int count = vm.getvmTaskMap().getOrDefault(task.getcloudletFunctionId(), 0);
        vm.getvmTaskMap().put(task.getcloudletFunctionId(), count+1);

    }

    public void submitCloudletoList(ServerlessTasks cloudlet) {
        getCloudletList().add(cloudlet);
    }

    /**
     * Insert the policy for selecting a VM when container concurrency is enableld
     */
    public ServerlessInvoker selectVM(ServerlessTasks cloudlet) {
        ServerlessInvoker selectedVm = null;
        boolean vmSelected = false;
        switch (Constants.vmSelectionAlgo) {
            /** Selecting Vm using RR method **/
            case "RR":
                for (int x = selectedVmIndex; x <= vmsCreatedList.size(); x++) {
                    ServerlessInvoker tempSelectedVm = (ServerlessInvoker) (ContainerVmList.getById(getVmsCreatedList(), x));
                    if (reschedule == true) {
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
                        if (reschedule == true) {
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
                if (reschedule == true) {
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
                    if (reschedule == true) {
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

                if (reschedule == true) {
                    tasksToReschedule.put(cloudlet, containerId);
                    // System.out.println(CloudSim.clock() + " Debug:Broker: Need to create a new container# " + containerId + " to reschedule cloudlet # " + cl.getCloudletId());

                }
            }

        return selectedVm;

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
                if(Math.ceil((cloudlet.getFinishTime()-((ServerlessTasks)cloudlet).getArrivalTime())) <=(Math.ceil(((ServerlessTasks) cloudlet).getMaxExecTime())) || Math.floor((cloudlet.getFinishTime()-((ServerlessTasks)cloudlet).getArrivalTime())) <=(Math.ceil(((ServerlessTasks) cloudlet).getMaxExecTime()))){
                    deadlineMetStat++;
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

            for (int i = 0; i < size; i++) {
                cloudlet = list.get(i);

                if (cloudlet.getCloudletStatusString() == "Success") {
                    String[] data = {String.valueOf(cloudlet.getCloudletId()),"SUCCESS",String.valueOf(cloudlet.getResourceId()), String.valueOf(cloudlet.getVmId()), String.valueOf(cloudlet.getActualCPUTime()), String.valueOf(cloudlet.getExecStartTime()), String.valueOf(cloudlet.getFinishTime()), String.valueOf((cloudlet.getFinishTime()-((ServerlessTasks)cloudlet).getArrivalTime())), ((ServerlessTasks)cloudlet).getResList(), String.valueOf(((ServerlessTasks) cloudlet).getMaxExecTime()), String.valueOf(((ServerlessTasks) cloudlet).getPriority())};
                    writer.writeNext(data);

                }
            }

            for(int x=0; x<getVmsCreatedList().size(); x++){
                String[] data = {"Vm # ", String.valueOf(getVmsCreatedList().get(x).getId()),String.valueOf(((ServerlessInvoker)getVmsCreatedList().get(x)).onTime),String.valueOf(((ServerlessInvoker)getVmsCreatedList().get(x)).offTime)};
                writer.writeNext(data);
            }


            // closing writer connection
            writer.close();
        }
        catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
