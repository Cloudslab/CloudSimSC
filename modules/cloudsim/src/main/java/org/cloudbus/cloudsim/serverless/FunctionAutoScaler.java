package org.cloudbus.cloudsim.serverless;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.ResCloudlet;
import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.core.ContainerHost;
import org.cloudbus.cloudsim.container.core.ContainerVm;
import org.cloudbus.cloudsim.container.core.containerCloudSimTags;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;

import java.util.*;

public class FunctionAutoScaler {

    /**
     * The DC.
     */
    private ServerlessDatacenter dc;
    private int userId ;
    private List<String> fnTypes = new ArrayList<>();
    public FunctionAutoScaler(ServerlessDatacenter dc){
        setServerlessDatacenter(dc);
    }


    public void setServerlessDatacenter(ServerlessDatacenter dc) {
        this.dc = dc;
    }

    public ServerlessDatacenter getServerlessDatacenter() {
        return dc;
    }

    public void scaleFunctions(){
        Map.Entry<Map<String, Map<String, Double>>, Map<String, ArrayList<ServerlessContainer>>> funcData = containerScalingTrigger();
        if (Constants.FUNCTION_HORIZONTAL_AUTOSCALING){
            containerHorizontalAutoScaler(funcData.getKey(), funcData.getValue());
        }
        if(Constants.FUNCTION_VERTICAL_AUTOSCALING) {
//            Map<String,Map<String, ArrayList<Integer>>> unAvailableActionMap =containerVerticalAutoScaling();
            List<? extends ContainerHost> list = dc.getVmAllocationPolicy().getContainerHostList();
            Map<String, Map<String, ArrayList<Integer>>> unAvailableActionMap = containerVerticalAutoScaler();
            Random rand = new Random();
            int cpuIncrement = 0;
            int memIncrement = 0;
            double cpuUtilization = 0;
            String scalingFunction = null;
            for (Map.Entry<String, Map<String, Double>> data : funcData.getKey().entrySet()) {
                if (data.getValue().get("container_count") > 0) {
                    if (data.getValue().get("container_cpu_util") / data.getValue().get("container_count") > Constants.CONTAINER_SCALE_CPU_THRESHOLD && data.getValue().get("container_cpu_util") / data.getValue().get("container_count") > cpuUtilization){
                        cpuUtilization = data.getValue().get("container_cpu_util") / data.getValue().get("container_count");
                        scalingFunction = data.getKey();

                    }
                }
            }
//            String scalingFunction = fnTypes.get(rand.nextInt(fnTypes.size()));

            if (scalingFunction != null){
                System.out.println("Now scaling function: "+scalingFunction);
                for (int x = 0; x < Constants.CONTAINER_MIPS_INCREMENT.length; x++) {
                    if (!unAvailableActionMap.get("cpuActions").get(scalingFunction).contains(Constants.CONTAINER_MIPS_INCREMENT[x])) {
                        cpuIncrement = Constants.CONTAINER_MIPS_INCREMENT[x];
                        break;
                    }
                }
                for (int x = 0; x < Constants.CONTAINER_RAM_INCREMENT.length; x++) {
                    if (!unAvailableActionMap.get("memActions").get(scalingFunction).contains(Constants.CONTAINER_RAM_INCREMENT[x])) {
                        memIncrement = Constants.CONTAINER_RAM_INCREMENT[x];
                        break;
                    }
                }
                for (ContainerHost host : list) {
                    for (ContainerVm machine : host.getVmList()) {
                        userId = machine.getUserId();
                        ServerlessInvoker vm = (ServerlessInvoker) machine;
                        if(vm.getFunctionContainerMap().containsKey(scalingFunction)){
                            for (Container cont : vm.getFunctionContainerMap().get(scalingFunction)) {
                                Log.printConcatLine(CloudSim.clock(), ": The Container #", cont.getId(),
                                        ", running on Vm #",cont.getVm().getId()
                                        , ", mem increment", memIncrement, ", cpu increment", cpuIncrement);
                                dc.containerVerticalScale(cont, vm, cpuIncrement, memIncrement);
                            }
                        }

                    }
            }

            }


        }
    }

    protected Map.Entry<Map<String, Map<String, Double>>, Map<String, ArrayList<ServerlessContainer>>> containerScalingTrigger() {
//        Map<String, String> funcData = new HashMap<>();
//        Map.Entry<Map<String, Map<String, Double>>, Map<String, ArrayList<ServerlessContainer>>> funcData = new HashMap<>();
        Map<String, Map<String, Double>> fnNestedMap = new HashMap<>();
        Map<String, ArrayList<ServerlessContainer>> emptyContainers = new HashMap<>();
        switch (Constants.SCALING_TRIGGER_LOGIC) {
            /** Triggering scaling based on cpu threshold method **/
            case "cpuThreshold":
                List<? extends ContainerHost> list = dc.getVmAllocationPolicy().getContainerHostList();
                for (ContainerHost host : list) {
                    for (ContainerVm machine : host.getVmList()) {
                        userId = machine.getUserId();
                        ServerlessInvoker vm = (ServerlessInvoker) machine;
                        for (Map.Entry<String, ArrayList<Container>> contMap : vm.getFunctionContainerMap().entrySet()) {

                            if (fnNestedMap.containsKey(contMap.getKey())) {
                                System.out.println("clock: " +CloudSim.clock()+" getFunctionContainerMap contains key: "+ contMap.getKey()+" so not adding: ");

                                fnNestedMap.get(contMap.getKey()).put("container_count", fnNestedMap.get(contMap.getKey()).get("container_count") + contMap.getValue().size());
                                if(contMap.getValue().size() > 0) {
                                    fnNestedMap.get(contMap.getKey()).put("container_MIPS", ((ServerlessContainer) ((contMap.getValue()).get(0))).getMips());
                                    fnNestedMap.get(contMap.getKey()).put("container_ram", Double.parseDouble(Float.toString((((ServerlessContainer) ((contMap.getValue()).get(0))).getRam()))));
                                    fnNestedMap.get(contMap.getKey()).put("container_PES", Double.parseDouble(Integer.toString((((ServerlessContainer) ((contMap.getValue()).get(0))).getNumberOfPes()))));
                                }
                            } else {
                                System.out.println("clock: " +CloudSim.clock()+" getFunctionContainerMap does not contain key: "+ contMap.getKey()+" so adding: ");

                                Map<String, Double> fnMap = new HashMap<>();
                                fnMap.put("container_count", (double) contMap.getValue().size());
                                fnMap.put("pending_container_count", 0.0);
                                fnMap.put("container_cpu_util", 0.0);
                                if(contMap.getValue().size() > 0){
                                    fnMap.put("container_MIPS", ((ServerlessContainer)((contMap.getValue()).get(0))).getMips());
                                    fnMap.put("container_ram", Double.parseDouble(Float.toString((((ServerlessContainer)((contMap.getValue()).get(0))).getRam()))));
                                    fnMap.put("container_PES", Double.parseDouble(Integer.toString((((ServerlessContainer)((contMap.getValue()).get(0))).getNumberOfPes()))));
                                }
                                else{
                                    fnMap.put("container_MIPS", 0.0);
                                    fnMap.put("container_ram", 0.0);
                                    fnMap.put("container_PES", 0.0);
                                }

                                fnNestedMap.put(contMap.getKey(), fnMap);
                            }
                            for (Container cont : contMap.getValue()) {
                                ServerlessContainer container = (ServerlessContainer) cont;
//                                System.out.println("clock: " +CloudSim.clock()+" container: "+ container.getId()+" running tasks: "+ container.getRunningTasks());

                                if (container.getRunningTasks().size() == 0) {
                                    if (!emptyContainers.containsKey(contMap.getKey())) {
                                        emptyContainers.put(contMap.getKey(), new ArrayList<>());
                                    }
                                    emptyContainers.get(contMap.getKey()).add(container);
                                }
                                fnNestedMap.get(contMap.getKey()).put("container_cpu_util", Double.sum(fnNestedMap.get(contMap.getKey()).get("container_cpu_util"), (((ServerlessRequestScheduler) (container.getContainerCloudletScheduler())).getTotalCurrentAllocatedMipsShareForRequests())));
                            }
                        }
                        for (Map.Entry<String, ArrayList<Container>> contMap : vm.getFunctionContainerMapPending().entrySet()) {
//                            System.out.println(contMap);
                            if (fnNestedMap.containsKey(contMap.getKey())) {
                                System.out.println("clock: " +CloudSim.clock()+" fnNestedMap contains key: "+ contMap.getKey()+" so not adding: ");

                                fnNestedMap.get(contMap.getKey()).put("pending_container_count", fnNestedMap.get(contMap.getKey()).get("pending_container_count") + contMap.getValue().size());

                                if(contMap.getValue().size() > 0) {
                                    fnNestedMap.get(contMap.getKey()).put("container_MIPS", ((ServerlessContainer) ((contMap.getValue()).get(0))).getMips());
                                    fnNestedMap.get(contMap.getKey()).put("container_ram", Double.parseDouble(Float.toString((((ServerlessContainer) ((contMap.getValue()).get(0))).getRam()))));
                                    fnNestedMap.get(contMap.getKey()).put("container_PES", Double.parseDouble(Integer.toString((((ServerlessContainer) ((contMap.getValue()).get(0))).getNumberOfPes()))));
                                }
                            } else {
                                System.out.println("clock: " +CloudSim.clock()+" fnNestedMap does not contain key: "+ contMap.getKey()+" so adding: ");

                                Map<String, Double> fnMap = new HashMap<>();
                                fnMap.put("pending_container_count", (double) contMap.getValue().size());
                                fnMap.put("container_count", 0.0);
                                fnMap.put("container_cpu_util", 0.0);
                                if(contMap.getValue().size() > 0){
                                    fnMap.put("container_MIPS", ((ServerlessContainer)((contMap.getValue()).get(0))).getMips());
                                    fnMap.put("container_ram", Double.parseDouble(Float.toString((((ServerlessContainer)((contMap.getValue()).get(0))).getRam()))));
                                    fnMap.put("container_PES", Double.parseDouble(Float.toString((((ServerlessContainer)((contMap.getValue()).get(0))).getNumberOfPes()))));
                                }
                                else{
                                    fnMap.put("container_MIPS", 0.0);
                                    fnMap.put("container_ram", 0.0);
                                    fnMap.put("container_PES", 0.0);
                                }

                                fnNestedMap.put(contMap.getKey(), fnMap);
                            }
                        }

                    }
                }

        }
        return  new AbstractMap.SimpleEntry<>(fnNestedMap, emptyContainers);
    }
    protected void containerHorizontalAutoScaler(Map<String, Map<String, Double>> fnNestedMap, Map<String, ArrayList<ServerlessContainer>> emptyContainers) {
        switch (Constants.HOR_SCALING_LOGIC) {
            /** Horizontal scaling based on cpu threshold method **/
            case "cpuThreshold":
                for (Map.Entry<String, Map<String, Double>> data : fnNestedMap.entrySet()) {
                    int desiredReplicas = 0;
//                    System.out.println(data);
                    if(data.getValue().get("container_count") > 0){
                        desiredReplicas = (int) Math.ceil(data.getValue().get("container_count") * (data.getValue().get("container_cpu_util") / data.getValue().get("container_count") / Constants.CONTAINER_SCALE_CPU_THRESHOLD));
                    }
                    int newReplicaCount;
                    int newReplicasToCreate;
                    int replicasToRemove;
                    newReplicaCount = Math.min(desiredReplicas, Constants.MAX_REPLICAS);
//                    if (desiredReplicas > 0) {
//                        newReplicaCount = Math.min(desiredReplicas, Constants.maxReplicas);
//                    } else {
//                        newReplicaCount = 1;
//                    }

                    System.out.println("clock: " +CloudSim.clock()+ "fn "+ data.getKey()+" Needed replica count: "+ newReplicaCount+" existing count: "+ (data.getValue().get("container_count") + data.getValue().get("pending_container_count")));
                    if (newReplicaCount > (data.getValue().get("container_count") + data.getValue().get("pending_container_count"))) {
                        newReplicasToCreate = (int) Math.ceil(newReplicaCount - data.getValue().get("container_count") - data.getValue().get("pending_container_count"));
                        for (int x = 0; x < newReplicasToCreate; x++) {
                            String[] dt = new String[5];
                            dt[0] = Integer.toString(userId);
                            dt[1] = data.getKey();
                            dt[2] = Double.toString(data.getValue().get("container_MIPS"));
                            dt[3] = Double.toString(data.getValue().get("container_ram"));
                            dt[4] = Double.toString(data.getValue().get("container_PES"));

                            dc.sendScaledContainerCreationRequest(dt);
                        }
                    }
                    if (newReplicaCount < (data.getValue().get("container_count") + data.getValue().get("pending_container_count"))) {
                        replicasToRemove = (int) Math.ceil(data.getValue().get("container_count") + data.getValue().get("pending_container_count") - newReplicaCount);
                        int removedContainers = 0;
                        if(emptyContainers.containsKey(data.getKey())){
                            for (ServerlessContainer cont : emptyContainers.get(data.getKey())) {
                                dc.getContainersToDestroy().add(cont);
//                            cont.setIdleStartTime(CloudSim.clock());
                                removedContainers++;
                                if (removedContainers == replicasToRemove) {
                                    break;
                                }
                            }
                        }

                    }

                }
        }
    }


    protected Map<String,Map<String, ArrayList<Integer>>> containerVerticalAutoScaler(){

        Map<String,Map<String, ArrayList<Integer>>> unAvailableActionMap =new HashMap<>();
        double peMIPSForContainerType = 0;
        double ramForContainerType = 0;
        double pesForContainerType = 0;
        Map<String, ArrayList<Integer>> unAVailableActionlistCPU = new HashMap<>();
        Map<String, ArrayList<Integer>> unAVailableActionlistRam = new HashMap<>();
        List<? extends ContainerHost> list = dc.getVmAllocationPolicy().getContainerHostList();
        for (ContainerHost host : list) {
            for (ContainerVm machine : host.getVmList()) {
                ServerlessInvoker vm = (ServerlessInvoker) machine;
                double vmUsedupRam = vm.getContainerRamProvisioner().getRam() - vm.getContainerRamProvisioner().getAvailableVmRam();
                double vmUsedupMIPS = vm.getContainerScheduler().getPeCapacity()*vm.getContainerScheduler().getPeList().size() - vm.getContainerScheduler().getAvailableMips();
                for (Map.Entry<String, ArrayList<Container>> contMap : vm.getFunctionContainerMap().entrySet()) {
                    double containerCPUUtilMin = 0;
                    double containerRAMUtilMin = 0;
                    int numContainers = contMap.getValue().size();
                    String functionId = contMap.getKey();
//                    if (!fnTypes.contains(functionId)){
//                        fnTypes.add(functionId);
//                    }
                    for (Container cont : contMap.getValue()) {
                        peMIPSForContainerType = cont.getMips();
                        ramForContainerType = cont.getRam();
                        pesForContainerType = cont.getNumberOfPes();
                        ServerlessContainer container = (ServerlessContainer)cont;
                        ServerlessRequestScheduler clScheduler = (ServerlessRequestScheduler) (container.getContainerCloudletScheduler());
                        if (clScheduler.getTotalCurrentAllocatedMipsShareForRequests() > containerCPUUtilMin){
                            containerCPUUtilMin = clScheduler.getTotalCurrentAllocatedMipsShareForRequests();
                        }
                        if (clScheduler.getTotalCurrentAllocatedRamForRequests() > containerRAMUtilMin){
                            containerRAMUtilMin = clScheduler.getTotalCurrentAllocatedRamForRequests();
                        }

                    }
                    for (int x = 0; x< (Constants.CONTAINER_RAM_INCREMENT).length; x++){
                        if(unAVailableActionlistRam.containsKey(functionId)){
                            if (!unAVailableActionlistRam.get(functionId).contains(x)){
                                if (Constants.CONTAINER_RAM_INCREMENT[x]*numContainers > vm.getContainerRamProvisioner().getAvailableVmRam() || Constants.CONTAINER_RAM_INCREMENT[x]*numContainers + vmUsedupRam < 0 || (ramForContainerType + Constants.CONTAINER_RAM_INCREMENT[x]) > Constants.MAX_CONTAINER_RAM || (ramForContainerType + Constants.CONTAINER_RAM_INCREMENT[x]) < Constants.MIN_CONTAINER_RAM || (ramForContainerType + Constants.CONTAINER_RAM_INCREMENT[x]) < containerRAMUtilMin* ramForContainerType){
                                    unAVailableActionlistRam.get(functionId).add(Constants.CONTAINER_RAM_INCREMENT[x]);
                                }
                            }

                        }
                        else{
                            ArrayList<Integer> listRam = new ArrayList<>();
                            unAVailableActionlistRam.put(functionId, listRam);
                        }

                    }
                    for (int x = 0; x< (Constants.CONTAINER_MIPS_INCREMENT).length; x++){
                        if(unAVailableActionlistCPU.containsKey(functionId)) {
                            if (!unAVailableActionlistCPU.get(functionId).contains(x)) {
                                if (Constants.CONTAINER_MIPS_INCREMENT[x] * numContainers * pesForContainerType > vm.getContainerScheduler().getAvailableMips() || Constants.CONTAINER_MIPS_INCREMENT[x] * numContainers * pesForContainerType + vmUsedupMIPS < 0 || (peMIPSForContainerType + Constants.CONTAINER_MIPS_INCREMENT[x]) > Constants.MAX_CONTAINER_MIPS || (peMIPSForContainerType + Constants.CONTAINER_MIPS_INCREMENT[x]) < Constants.MIN_CONTAINER_MIPS || (peMIPSForContainerType + Constants.CONTAINER_MIPS_INCREMENT[x]) < containerCPUUtilMin * peMIPSForContainerType) {
                                    unAVailableActionlistCPU.get(functionId).add(Constants.CONTAINER_MIPS_INCREMENT[x]);
                                }
                            }
                        }
                        else{
                            ArrayList<Integer> listCpu = new ArrayList<>();
                            unAVailableActionlistCPU.put(functionId, listCpu);
                        }
                    }

                }



            }
        }
        System.out.println("cpu unavailable: "+unAVailableActionlistCPU);
        System.out.println("mem unavailable: "+ unAVailableActionlistRam);
        unAvailableActionMap.put("cpuActions", unAVailableActionlistCPU);
        unAvailableActionMap.put("memActions", unAVailableActionlistRam);
        return unAvailableActionMap;
//        add the logic to randomy select a cpu and memory change value for each function type

    }

}
