package org.cloudbus.cloudsim.serverless;

import org.cloudbus.cloudsim.container.containerProvisioners.ContainerBwProvisioner;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerPe;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerRamProvisioner;
import org.cloudbus.cloudsim.container.core.PowerContainerVm;
import org.cloudbus.cloudsim.container.schedulers.ContainerScheduler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ServerlessInvoker extends PowerContainerVm {

    /**
     * The task type map of vm
     */

    private Map<String, Integer> vmTaskMap = new HashMap<>();

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

    public void setStatus(String vmStatus){
        status = vmStatus;
    }

    public void setRecordTime(double time){
        recordTime = time;
    }

    public double getRecordTime(){
        return recordTime;
    }
}
