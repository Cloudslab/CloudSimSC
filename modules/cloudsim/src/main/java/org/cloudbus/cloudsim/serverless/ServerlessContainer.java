package org.cloudbus.cloudsim.serverless;

import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.schedulers.ContainerCloudletScheduler;

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
     * @param containerCloudletScheduler
     * @param schedulingInterval
     */

    boolean newContainer = false;
    public boolean reschedule = false;
    public ServerlessContainer(int id, int userId, double mips, int numberOfPes, int ram, long bw, long size, String containerManager, ContainerCloudletScheduler containerCloudletScheduler, double schedulingInterval, boolean newCont, boolean reschedule) {
        super(id, userId, mips, numberOfPes, ram, bw, size, containerManager, containerCloudletScheduler, schedulingInterval);
        this.newContainer = newCont;
        setReschedule(reschedule);
    }
    public void setReschedule(boolean reschedule){this.reschedule = reschedule;}
    public boolean getReschedule() {return reschedule;}




    public double updateContainerProcessing(double currentTime, List<Double> mipsShare, ServerlessInvoker vm) {
        if (mipsShare != null) {
            return ((ServerlessCloudletScheduler) getContainerCloudletScheduler()).updateContainerProcessing(currentTime, mipsShare,vm);
        }
        return 0.0;
    }
}
