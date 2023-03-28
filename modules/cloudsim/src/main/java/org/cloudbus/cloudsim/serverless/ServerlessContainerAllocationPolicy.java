package org.cloudbus.cloudsim.serverless;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.core.ContainerVm;
import org.cloudbus.cloudsim.container.resourceAllocators.PowerContainerAllocationPolicy;
import org.cloudbus.cloudsim.core.CloudSim;

import java.util.List;
import java.util.Map;

public class ServerlessContainerAllocationPolicy extends PowerContainerAllocationPolicy {
    public ServerlessContainerAllocationPolicy() {
        super();
    }

    public List<Map<String, Object>> optimizeAllocation(List<? extends Container> containerList) {
        return null;
    }

    public boolean allocateVmForContainer(Container container, ContainerVm containerVm, List<ContainerVm> containerVmList) {
        setContainerVmList(containerVmList);

        if (containerVm == null) {
            Log.formatLine("%.2f: No suitable VM found for Container#" + container.getId() + "\n", CloudSim.clock());
            return false;
        }
        if (containerVm.containerCreate(container)) { // if vm has been succesfully created in the host
            getContainerTable().put(container.getUid(), containerVm);
//                container.setVm(containerVm);
            Log.formatLine(
                    "%.2f: Container #" + container.getId() + " has been allocated to the VM #" + containerVm.getId(),
                    CloudSim.clock());
            return true;
        }
        Log.formatLine(
                "%.2f: Creation of Container #" + container.getId() + " on the Vm #" + containerVm.getId() + " failed\n",
                CloudSim.clock());
        return false;
    }

    public boolean reallocateVmResourcesForContainer(Container container, ServerlessInvoker vm, ServerlessTasks cl) {
        boolean result = vm.reallocateResourcesForContainer(container, cl);

        return result;

    }
}