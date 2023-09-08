package org.cloudbus.cloudsim.serverless;

import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.schedulers.ContainerCloudletScheduler;

import java.util.ArrayList;
import java.util.List;

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
     * @param containerrequestScheduler
     * @param schedulingInterval
     */

    /**
     * The pending task list for the container
     */
    private  ArrayList<ServerlessRequest> pendingTasks = new ArrayList<>();
    /**
     * The running task list for the container
     */
    private  ArrayList<ServerlessRequest> runningTasks = new ArrayList<>();

    /**
     * The running task list for the container
     */
    private  ArrayList<ServerlessRequest> finishedTasks = new ArrayList<>();
    /**
     * Container type
     */
    private  String functionType = null;
    boolean newContainer = false;
    private boolean reschedule = false;
    private boolean idling = false;

    private double startTime = 0;
    private double finishTime = 0;
    private double idleStartTime = 0;
    public ServerlessContainer(int id, int userId, String type, double mips, int numberOfPes, int ram, long bw, long size, String containerManager, ContainerCloudletScheduler containerRequestScheduler, double schedulingInterval, boolean newCont, boolean idling, boolean reschedule, double idleStartTime, double startTime, double finishTime) {
        super(id, userId, mips, numberOfPes, ram, bw, size, containerManager, containerRequestScheduler, schedulingInterval);
        this.newContainer = newCont;
        setReschedule(reschedule);
        setIdling(idling);
        setType(type);
        setIdleStartTime(idleStartTime);
    }
    public void setReschedule(boolean reschedule){this.reschedule = reschedule;}
    public void setIdling(boolean idling){this.idling = idling;}

    public void setIdleStartTime(double time){this.idleStartTime = time;}
    public void setStartTime(double time){this.startTime = time;}
    public void setFinishTime(double time){this.finishTime = time;}
    public void setType(String type){this.functionType = type;}
    public void setPendingTask(ServerlessRequest task){
        pendingTasks.add(task);
    }
    public void setRunningTask(ServerlessRequest task){runningTasks.add(task); }

    public void setfinishedTask(ServerlessRequest task){finishedTasks.add(task); }
    public boolean getReschedule() {return reschedule;}
    public boolean getIdling() {return idling;}
    public double getIdleStartTime(){return idleStartTime;}
    public double getStartTime(){return startTime;}
    public double getFinishTime(){return finishTime;}
    public ServerlessRequest getPendingTask(int index){
        return pendingTasks.get(index);
    }
    public ServerlessRequest getRunningTask(int index){
        return runningTasks.get(index);
    }
    public ArrayList<ServerlessRequest> getfinishedTasks(){
        return finishedTasks;
    }
    public ArrayList<ServerlessRequest> getRunningTasks(){
        return runningTasks;
    }
    public String getType() {return functionType;}




    public double updateContainerProcessing(double currentTime, List<Double> mipsShare, ServerlessInvoker vm) {
        if (mipsShare != null) {
            return ((ServerlessRequestScheduler) getContainerCloudletScheduler()).updateContainerProcessing(currentTime, mipsShare,vm);
        }
        return 0.0;
    }
}
