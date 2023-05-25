package org.cloudbus.cloudsim.serverless;

import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.schedulers.ContainerCloudletScheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ServerlessContainer extends Container {
    /**
     * Creates a new Container object.
     *
     * @param id
     * @param userId
     * @param mips
     * @param numberOfPes
     * @param ram
     * @param bw
     * @param size
     * @param containerManager
     * @param containerCloudletScheduler
     * @param schedulingInterval
     */

    /**
     * The pending task list for the container
     */
    private  ArrayList<ServerlessTasks> pendingTasks = new ArrayList<>();
    /**
     * The running task list for the container
     */
    private  ArrayList<ServerlessTasks> runningTasks = new ArrayList<>();
    /**
     * Container type
     */
    private  String functionType = null;
    boolean newContainer = false;
    public boolean reschedule = false;
    public ServerlessContainer(int id, int userId, String type, double mips, int numberOfPes, int ram, long bw, long size, String containerManager, ContainerCloudletScheduler containerCloudletScheduler, double schedulingInterval, boolean newCont, boolean reschedule) {
        super(id, userId, mips, numberOfPes, ram, bw, size, containerManager, containerCloudletScheduler, schedulingInterval);
        this.newContainer = newCont;
        setReschedule(reschedule);
        setType(type);
    }
    public void setReschedule(boolean reschedule){this.reschedule = reschedule;}
    public void setType(String type){this.functionType = type;}
    public void setPendingTask(ServerlessTasks task){
        pendingTasks.add(task);
    }
    public void setRunningTask(ServerlessTasks task){runningTasks.add(task); }
    public boolean getReschedule() {return reschedule;}
    public ServerlessTasks getPendingTask(int index){
        return pendingTasks.get(index);
    }
    public ServerlessTasks getRunningTask(int index){
        return runningTasks.get(index);
    }
    public ArrayList<ServerlessTasks> getRunningTasks(){
        return runningTasks;
    }
    public String getType() {return functionType;}




    public double updateContainerProcessing(double currentTime, List<Double> mipsShare, ServerlessInvoker vm) {
        if (mipsShare != null) {
            return ((ServerlessCloudletScheduler) getContainerCloudletScheduler()).updateContainerProcessing(currentTime, mipsShare,vm);
        }
        return 0.0;
    }
}
