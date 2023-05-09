package org.cloudbus.cloudsim.serverless;

import org.cloudbus.cloudsim.container.containerProvisioners.ContainerBwProvisioner;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerPe;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerRamProvisioner;
import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.core.PowerContainerVm;
import org.cloudbus.cloudsim.container.schedulers.ContainerScheduler;

import java.util.*;

public class ServerlessInvoker extends PowerContainerVm {
    /**
     * The container type map of vm - contains the function type and the container list
     */
    private  Map<String, ArrayList<Container>> functionContainerMap = new HashMap<String, ArrayList<Container>>();

    /**
     * The task type map of vm - contains the function type and the task count
     */

    private Map<String, Integer> vmTaskMap = new HashMap<>();
    /**
     * Records the cloudlets in execution as per the execution order
     */

    private Stack<ServerlessTasks> runningCloudletStack = new Stack<ServerlessTasks>();
    private  ArrayList<ServerlessTasks> runningCloudletList = new ArrayList<>();

    /**
     * The task  map of vm - contains the function type and the task
     */
    private Map<String, ArrayList<ServerlessTasks>> vmTaskExecutionMap = new HashMap<>();
    private Map<String, ArrayList<ServerlessTasks>> vmTaskExecutionMapFull = new HashMap<>();

    /**
     * On Off status of VM
     */

    private  String status = null;

    /**
     * On Off status record time of VM
     */

    private  double recordTime = 0;

    public double onTime = 0;
    public double offTime  = 0;
    public boolean used = false;

    public ServerlessInvoker(int id, int userId, double mips, float ram, long bw, long size, String vmm, ContainerScheduler containerScheduler, ContainerRamProvisioner containerRamProvisioner, ContainerBwProvisioner containerBwProvisioner, List<? extends ContainerPe> peList, double schedulingInterval) {
        super(id, userId, mips, ram, bw, size, vmm, containerScheduler, containerRamProvisioner, containerBwProvisioner, peList, schedulingInterval);
    }

    public Map<String, Integer> getvmTaskMap(){return vmTaskMap;}

    public Map<String, ArrayList<ServerlessTasks>> getVmTaskExecutionMap(){return vmTaskExecutionMap;}
    public Map<String, ArrayList<ServerlessTasks>> getVmTaskExecutionMapFull(){return vmTaskExecutionMapFull;}
    public String getStatus(){
        return status;
    }
    public ArrayList<ServerlessTasks> getRunningCloudletList() {
        return runningCloudletList;
    }

    public void setStatus(String vmStatus){
        status = vmStatus;
    }

    public void setRecordTime(double time){
        recordTime = time;
    }

    public double getRecordTime(){
        return recordTime;
    }

    public void setFunctionContainerMap(Container container, String functionId){
        //System.out.println("Debug: Before: Set map "+ this.getId()+" "+functionContainerMap);

        if(!functionContainerMap.containsKey(functionId)){
            ArrayList<Container> containerListMap = new ArrayList<>();
            containerListMap.add(container);
            functionContainerMap.put(functionId,containerListMap );
        }
        else {
            if(!functionContainerMap.get(functionId).contains(container)){
                functionContainerMap.get(functionId).add(container);
            }


        }
    }

    public void addToVmTaskExecutionMap(ServerlessTasks task, ServerlessInvoker vm){

        /** Adding to full task map **/
        int countFull = vmTaskExecutionMapFull.containsKey(task.getcloudletFunctionId()) ? (vmTaskExecutionMapFull.get(task.getcloudletFunctionId())).size(): 0;

        if(countFull == 0){
            ArrayList<ServerlessTasks> taskListFull = new ArrayList<>();
            taskListFull.add(task);
            vmTaskExecutionMapFull.put(task.getcloudletFunctionId(),taskListFull);
        }
        else{
            vmTaskExecutionMapFull.get(task.getcloudletFunctionId()).add(task);
        }

        /** Adding to moving average task map **/

        int count = vmTaskExecutionMap.containsKey(task.getcloudletFunctionId()) ? (vmTaskExecutionMap.get(task.getcloudletFunctionId())).size(): 0;
        //vm.getVmTaskExecutionMap().put(task.getcloudletFunctionId(), count+1);

        if(count == 0){
            ArrayList<ServerlessTasks> taskList = new ArrayList<>();
            taskList.add(task);
            vmTaskExecutionMap.put(task.getcloudletFunctionId(),taskList);
        }
        else{
            vmTaskExecutionMap.get(task.getcloudletFunctionId()).add(task);
        }

        if(count ==Constants.WINDOW_SIZE){
            vmTaskExecutionMap.get(task.getcloudletFunctionId()).remove(0);
        }



    }



    public Map<String, ArrayList<Container>> getFunctionContainerMap(){
        return functionContainerMap;
    }
}
