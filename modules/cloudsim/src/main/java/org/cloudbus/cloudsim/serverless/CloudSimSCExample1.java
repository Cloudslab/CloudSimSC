package org.cloudbus.cloudsim.serverless;/*
 * Title:        CloudSimSC Toolkit
 * Description:  CloudSimSC Toolkit for Modeling and Simulation
 *               of Serverless Clouds
 *
 * Copyright (c) 2023, The University of Melbourne, Australia
 */


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;

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
import org.cloudbus.cloudsim.container.resourceAllocatorMigrationEnabled.PCVmAllocationPolicyMigrationAbstractHostSelection;
import org.cloudbus.cloudsim.container.resourceAllocators.ContainerVmAllocationPolicy;
import org.cloudbus.cloudsim.container.schedulers.ContainerVmSchedulerTimeSharedOverSubscription;
import org.cloudbus.cloudsim.container.utils.IDs;
import org.cloudbus.cloudsim.container.vmSelectionPolicies.PowerContainerVmSelectionPolicy;
import org.cloudbus.cloudsim.container.vmSelectionPolicies.PowerContainerVmSelectionPolicyMaximumUsage;
import org.cloudbus.cloudsim.core.CloudSim;

/**
 * Simulation setup for Serverless Function execution
 * Case Study 1:
 * This test covers featured such as,
 * 1. Request load balancing
 * 2. Function scheduling
 *
 * @author Anupama Mampage
 */

/**
 * Config properties
 * containerConcurrency -> false
 * functionAutoScaling -> false
 * functionHorizontalAutoscaling -> false
 * funtionVerticalAutoscaling -> false
 * scalePerRequest -> true/false
 * containerIdlingEnabled -> false/true
 *
 *
 *  Case Study 2:
 *  This test covers featured such as,
 *  1. Function vertical auto-scaling
 *  2. Function horizontal auto-scaling
 *  3. Container concurrency
 *
 *  Config properties
 *  containerConcurrency -> true
 *  functionAutoScaling -> true
 *  functionHorizontalAutoscaling -> false/true
 *  funtionVerticalAutoscaling -> false/true
 *  scalePerRequest -> false
 *  containerIdlingEnabled -> false
 *
 */

public class CloudSimSCExample1 {


    /** The vmlist. */
    private static List<ServerlessInvoker> vmList;

//    private int overBookingfactor = 80;
    private static int controllerId;

    private static RequestLoadBalancer loadBalancer;

    private static ServerlessDatacenter DC;

    private static ServerlessController controller;

    /**
     * Creates main() to run this example
     */
    public static void main(String[] args) {

        Log.printLine("Starting CloudSimSCExample1...");

        try {
            // First step: Initialize the CloudSim package. It should be called
            // before creating any entities.
            int num_user = 1;   // number of cloud users
            Calendar calendar = Calendar.getInstance();
            boolean trace_flag = false;  // mean trace events

            // Initialize the CloudSim library
            CloudSim.init(num_user, calendar, trace_flag);

            controller = createBroker();
            controllerId = controller.getId();

            // Second step: Create Datacenters
            //Datacenters are the resource providers in CloudSim. We need at least one of them to run a CloudSim simulation
            DC = createDatacenter("datacenter");

            //Third step: Create the virtual machines
            vmList = createVmList(controllerId);

            //Fourth step: submit vm list to the broker
            controller.submitVmList(vmList);

            //Fifth step: Create a load balancer
            loadBalancer = new RequestLoadBalancer(controller, DC);
            controller.setLoadBalancer(loadBalancer);
            controller.setServerlessDatacenter(DC);


            //Sixth step: Create the request workload
            createRequests();

//          the time at which the simulation has to be terminated.
            CloudSim.terminateSimulation(2500.00);

//          Starting the simualtion
            CloudSim.startSimulation();

//          Stopping the simualtion.
            CloudSim.stopSimulation();


//          Printing the results when the simulation is finished.
            List<ContainerCloudlet> finishedRequests = controller.getCloudletReceivedList();
            List<ServerlessContainer> destroyedContainers = controller.getContainersDestroyedList();
            printRequestList(finishedRequests);
            printContainerList(destroyedContainers);
            if (Constants.MONITORING){
                printVmUpDownTime();
                printVmUtilization();
            }

//          Writing the results to a file when the simulation is finished.
            writeDataLineByLine(finishedRequests);


            Log.printLine("ContainerCloudSimExample1 finished!");
        } catch (Exception e) {
            e.printStackTrace();
            Log.printLine("Unwanted errors happen");
        }
    }

    private static void createRequests() throws IOException {
        long fileSize = 300L;
        long outputSize = 300L;
        int createdRequests = 0;
        BufferedReader br = new BufferedReader(new FileReader(Constants.FUNCTION_REQUESTS_FILENAME));
        String line = null;
        String cvsSplitBy = ",";
        controller.noOfTasks++;

//        Serverless requests could utilize part of a vCPU core in case container concurrency is enabled
        UtilizationModelPartial utilizationModelPar = new UtilizationModelPartial();
        UtilizationModelFull utilizationModel = new UtilizationModelFull();

        while ((line = br.readLine()) != null) {
            String[] data = line.split(cvsSplitBy);
            ServerlessRequest request = null;

            try {
                request = new ServerlessRequest(IDs.pollId(ServerlessRequest.class), Double.parseDouble(String.valueOf(data[0])), String.valueOf(data[1]), Long.parseLong(data[2]), Integer.parseInt(data[3]), Integer.parseInt(data[4]), Long.parseLong(data[5]), Double.parseDouble(data[6]), Double.parseDouble(data[7]),
                        fileSize, outputSize, utilizationModelPar, utilizationModelPar, utilizationModel,  0, true);
                System.out.println("request No " + request.getCloudletId());
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(0);
            }

            request.setUserId(controller.getId());
            System.out.println(CloudSim.clock() + " request created. This request arrival time is :" + Double.parseDouble(data[0]));
            controller.requestArrivalTime.add(Double.parseDouble(data[0]) + Constants.FUNCTION_SCHEDULING_DELAY);
//            requestList.add(request);
            controller.requestQueue.add(request);
            createdRequests += 1;

        }
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

    private static ServerlessController createBroker() {

        ServerlessController controller = null;
        int overBookingFactor = 80;

        try {
            controller = new ServerlessController("Broker", overBookingFactor);
        } catch (Exception var2) {
            var2.printStackTrace();
            System.exit(0);
        }

        return controller;
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
                hostSelectionPolicy, Constants.OVER_UTILIZATION_THRESHOLD, Constants.UNDER_UTILIZATION_THRESHOLD);
        //      Allocating vms to container
        FunctionScheduler containerAllocationPolicy = new FunctionScheduler();

        ContainerDatacenterCharacteristics characteristics = new
                ContainerDatacenterCharacteristics(arch, os, vmm, hostList, time_zone, cost, costPerMem, costPerStorage,
                costPerBw);
        /** Set datacenter monitoring to true if metrics monitoring is required **/
        ServerlessDatacenter datacenter = new ServerlessDatacenter(name, characteristics, vmAllocationPolicy,
                containerAllocationPolicy, new LinkedList<Storage>(), Constants.SCHEDULING_INTERVAL, getExperimentName("SimTest1", String.valueOf(80)), logAddress,
                Constants.VM_STARTTUP_DELAY, Constants.CONTAINER_STARTTUP_DELAY, Constants.MONITORING);

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


    private static void printContainerList(List<ServerlessContainer> list) {
        int size = list.size();
        ServerlessContainer container;
        int deadlineMetStat = 0;

        String indent = "    ";
        Log.printLine();
        Log.printLine("========== OUTPUT ==========");
        Log.printLine("Container ID" + indent + "VM ID" + indent + "Start Time" + indent
                + "Finish Time" + indent + "Finished Requests List");

        DecimalFormat dft = new DecimalFormat("###.##");
        for (int i = 0; i < size; i++) {
            container = list.get(i);
            Log.print(indent + container.getId() + indent + indent);

            Log.printLine(indent + (container.getVm()).getId() + indent + indent + (dft.format(container.getStartTime()))
                    + indent + indent + indent +indent +  dft.format(container.getFinishTime())
                    + indent + indent+ indent+ indent
                    +  container.getfinishedTasks());


        }

        Log.printLine("Deadline met no: "+deadlineMetStat);

    }

    /**
     * Prints the request objects
     * @param list  list of requests
     */
    private static void printRequestList(List<ContainerCloudlet> list) {
        int size = list.size();
        Cloudlet request;
        int deadlineMetStat = 0;
        int totalResponseTime = 0;
        int failedRequestRatio = 0;
        float averageResponseTime = 0;
        int totalRequests = 0;
        int failedRequests = 0;

        String indent = "    ";
        Log.printLine();
        Log.printLine("========== OUTPUT ==========");
        Log.printLine("request ID" + indent +"Function ID" + indent+"Container ID" + indent + "STATUS" + indent
                + "Data center ID" + indent + "Final VM ID" + indent + "Execution Time" + indent
                + "Start Time" + indent + "Finish Time"+ indent + "Response Time"+ indent + "Vm List");

        DecimalFormat dft = new DecimalFormat("###.##");
        for (int i = 0; i < size; i++) {
            request = list.get(i);
            Log.print(indent + request.getCloudletId() + indent + indent);
            Log.print(indent + ((ServerlessRequest)request).getRequestFunctionId() + indent + indent);
            Log.print(indent + ((ServerlessRequest)request).getContainerId() + indent + indent);
            totalRequests += 1;

            if (request.getCloudletStatusString() == "Success") {
                totalResponseTime += request.getFinishTime()-((ServerlessRequest)request).getArrivalTime();
                Log.print("SUCCESS");
                if (Math.ceil((request.getFinishTime() - ((ServerlessRequest) request).getArrivalTime())) <= (Math.ceil(((ServerlessRequest) request).getMaxExecTime())) || Math.floor((request.getFinishTime() - ((ServerlessRequest) request).getArrivalTime())) <= (Math.ceil(((ServerlessRequest) request).getMaxExecTime()))) {
                    deadlineMetStat++;
                }
            }
            else{
                Log.print("DROPPED");
                failedRequests += 1;
            }

            Log.printLine(indent + indent + request.getResourceId()
                    + indent + indent + indent +indent + request.getVmId()
                    + indent + indent+ indent+ indent
                    + dft.format(request.getActualCPUTime()) + indent+ indent
                    + indent + dft.format(request.getExecStartTime())
                    + indent + indent+ indent
                    + dft.format(request.getFinishTime())+ indent + indent + indent
                    + dft.format(request.getFinishTime()-((ServerlessRequest)request).getArrivalTime())+ indent + indent + indent
                    + ((ServerlessRequest)request).getResList());


        }

        Log.printLine("Deadline met no: "+deadlineMetStat);



    }

    private static void printVmUpDownTime(){
        double totalVmUpTime = 0;

        for(int x=0; x<controller.getVmsCreatedList().size(); x++){
            ServerlessInvoker vm = ((ServerlessInvoker)controller.getVmsCreatedList().get(x));
            if(vm.getStatus().equals("OFF")){
                vm.offTime += 2500.00-vm.getRecordTime();
            }
            else if(vm.getStatus().equals("ON")){
                vm.onTime += 2500.00-vm.getRecordTime();
            }
            System.out.println("Vm # "+controller.getVmsCreatedList().get(x).getId()+" used: "+vm.used );
            System.out.println("Vm # "+controller.getVmsCreatedList().get(x).getId()+"has uptime of: "+vm.onTime);
            System.out.println("Vm # "+controller.getVmsCreatedList().get(x).getId()+"has downtime of: "+vm.offTime);
            totalVmUpTime +=vm.onTime;
        }
    }

    private static void printVmUtilization(){
        System.out.println("Average CPU utilization of vms: "+ controller.getAverageResourceUtilization());
        System.out.println("Average vm count: "+ controller.getAverageVmCount());
        System.out.println("Using exsiting cont: "+ controller.exsitingContCount);

    }

    private static void writeDataLineByLine(List<ContainerCloudlet> list) throws ParameterException {
        // first create file object for file placed at location
        // specified by filepath
        int size = list.size();
        int successfulRequestCount = 0;
        int droppedRequestCount = 0;
        double totalResponseTime = 0;
        float failedRequestRatio = 0;
        float averageResponseTime = 0;
        Cloudlet request;
        String indent = "    ";
//        java.io.File file = new java.io.File("D:\\OneDrive - The University of Melbourne\\UniMelb\\Studying\\Serverless simulator\\Data\\Result.csv");
        java.io.File file = new java.io.File("Result.csv");


        try {
            // create FileWriter object with file as parameter
            FileWriter outputfile = new FileWriter(file);

            // create CSVWriter object filewriter object as parameter
            CSVWriter writer = new CSVWriter(outputfile);

            // adding header to csv
            String[] header = { "request ID", "Function ID", "STATUS", "Container ID", "Data center ID","Final VM ID" ,"Execution Time" ,"Start Time" ,"Finish Time","Response Time","Vm List"};
            writer.writeNext(header);

            for (ContainerCloudlet containerRequest : list) {
                request = containerRequest;

                if (request.getCloudletStatusString().equals("Success")) {
                    totalResponseTime += (request.getFinishTime() - ((ServerlessRequest) request).getArrivalTime());
                    successfulRequestCount++;
                    String[] data = {String.valueOf(request.getCloudletId()), String.valueOf(((ServerlessRequest)request).getRequestFunctionId()), "SUCCESS",  String.valueOf(((ServerlessRequest)request).getContainerId()), String.valueOf(request.getResourceId()), String.valueOf(request.getVmId()), String.valueOf(request.getActualCPUTime()), String.valueOf(request.getExecStartTime()), String.valueOf(request.getFinishTime()), String.valueOf((request.getFinishTime() - ((ServerlessRequest) request).getArrivalTime())), ((ServerlessRequest) request).getResList()};
                    writer.writeNext(data);

                } else {
                    droppedRequestCount++;
                    String[] data = {String.valueOf(request.getCloudletId()), String.valueOf(((ServerlessRequest)request).getRequestFunctionId()), "DROPPED", String.valueOf(request.getResourceId()), String.valueOf(request.getVmId()), String.valueOf(request.getActualCPUTime()), String.valueOf(request.getExecStartTime()), String.valueOf(request.getFinishTime()), String.valueOf((request.getFinishTime() - ((ServerlessRequest) request).getArrivalTime())), ((ServerlessRequest) request).getResList()};
                    writer.writeNext(data);
                }
            }

            if (Constants.MONITORING){
                for(int x=0; x<controller.getVmsCreatedList().size(); x++){
                    String[] data = {"Vm # ", String.valueOf(controller.getVmsCreatedList().get(x).getId()),"On time  ", String.valueOf(((ServerlessInvoker)controller.getVmsCreatedList().get(x)).onTime),"Off time  ", String.valueOf(((ServerlessInvoker)controller.getVmsCreatedList().get(x)).offTime)};
                    writer.writeNext(data);
                }
            }

            String[] data1 = {"Average VM utilization  ", String.valueOf(controller.getAverageResourceUtilization())};
            writer.writeNext(data1);
            String[] data2 = {"Average VM Count # ", String.valueOf(controller.getAverageVmCount())};
            writer.writeNext(data2);
            String[] data3 = {"Successful Request Count # ", String.valueOf(successfulRequestCount)};
            writer.writeNext(data3);
            String[] data4 = {"Dropped Request Count # ", String.valueOf(droppedRequestCount)};
            writer.writeNext(data4);
            totalResponseTime = totalResponseTime/successfulRequestCount;
            String[] data5 = {"Average Request Response Time # ", String.valueOf(totalResponseTime)};
            writer.writeNext(data5);
            failedRequestRatio = (float) droppedRequestCount /(successfulRequestCount + droppedRequestCount);
            String[] data6 = {"Dropped Request Ratio # ", String.valueOf(failedRequestRatio)};
            writer.writeNext(data6);



            // closing writer connection
            writer.close();
        }
        catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }


}
