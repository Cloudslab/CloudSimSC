package org.cloudbus.cloudsim.serverless;

import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.container.core.ContainerCloudlet;
import org.cloudbus.cloudsim.core.CloudSim;

public class ServerlessTasks extends ContainerCloudlet {

    private String cloudletType = null;
    private String cloudletFunctionId = null;
    private int cloudletMemory = 0;
    private double maxExecTime = 0;
    private double arrivalTime = 0;
    public boolean reschedule = false;
    private int priority = 0;

    public ServerlessTasks(int cloudletId, double arrivalTime, String cloudletType, String cloudletFunctionId, long cloudletLength, int pesNumber, int memory, double maxExecTime, int priority, long cloudletFileSize, long cloudletOutputSize, UtilizationModel utilizationModelCpu, UtilizationModel utilizationModelRam, UtilizationModel utilizationModelBw) {
        super(cloudletId, cloudletLength, pesNumber, cloudletFileSize, cloudletOutputSize, utilizationModelCpu, utilizationModelRam, utilizationModelBw);

        setCloudletType(cloudletType);
        setCloudletFunctionId(cloudletFunctionId);
        setcloudletMemory(memory);
        setMaxExecTime(maxExecTime);
        setArrivalTime(arrivalTime);
        setPriority(priority);
    }

    public void setReschedule(boolean value){this.reschedule = value;}
    public void setCloudletType(String cloudletType){this.cloudletType = cloudletType;}
    public void setPriority(int priority){this.priority = priority;}
    public void setCloudletFunctionId(String cloudletFunctionId){this.cloudletFunctionId = cloudletFunctionId;}
    public void setcloudletMemory(int cloudletMemory){this.cloudletMemory = cloudletMemory;}
    public void setMaxExecTime(double maxExecTime){this.maxExecTime = maxExecTime;}
    public void setArrivalTime(double arrivalTime){this.arrivalTime = arrivalTime;}

    public String getcloudletType() {return cloudletType;}
    public String getcloudletFunctionId() {return cloudletFunctionId;}
    public int getcloudletMemory() {return cloudletMemory;}
    public double getMaxExecTime() {return maxExecTime;}
    public int getPriority() {return priority;}
    public double getArrivalTime() {return arrivalTime;}


    public void setResourceParameter(final int resourceID, final double cost, int vmId) {
        final Resource res = new Resource();
        res.vmId = vmId;
        res.resourceId = resourceID;
        res.costPerSec = cost;
        res.resourceName = CloudSim.getEntityName(resourceID);

        // add into a list if moving to a new grid resource
        resList.add(res);

        if (index == -1 && record) {
            write("Allocates this Cloudlet to " + res.resourceName + " (ID #" + resourceID
                    + ") with cost = $" + cost + "/sec");
        } else if (record) {
            final int id = resList.get(index).resourceId;
            final String name = resList.get(index).resourceName;
            write("Moves Cloudlet from " + name + " (ID #" + id + ") to " + res.resourceName + " (ID #"
                    + resourceID + ") with cost = $" + cost + "/sec");
        }

        index++;  // initially, index = -1
    }


    public void setResourceParameter(final int resourceID, final double costPerCPU, final double costPerBw, int vmId) {
        setResourceParameter(resourceID, costPerCPU, vmId);
        this.costPerBw = costPerBw;
        accumulatedBwCost = costPerBw * getCloudletFileSize();
    }

    public String getResList() {
        String resString = "";
        for(int x=0; x<resList.size(); x++){
            if(x==resList.size()-1){
                resString = resString.concat(Integer.toString(resList.get(x).vmId)) ;
            }
            else
                resString = resString.concat(Integer.toString(resList.get(x).vmId)+" ,") ;

        }
        return resString;
    }

}

