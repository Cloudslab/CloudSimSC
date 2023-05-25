package org.cloudbus.cloudsim.serverless;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.core.ContainerVm;
import org.cloudbus.cloudsim.container.lists.ContainerVmList;
import org.cloudbus.cloudsim.container.resourceAllocators.PowerContainerAllocationPolicy;
import org.cloudbus.cloudsim.core.CloudSim;

import java.util.List;
import java.util.Map;
import java.util.Random;


/**
 * FunctionScheduler class for CloudSimServerless extension. This class represents the scheduling policy of a function instance on a VM
 *
 * @author Anupama Mampage
 */
public class FunctionScheduler extends PowerContainerAllocationPolicy {

    /** Vm index for selecting Vm in round robin fashion **/
    private int selectedVmIndex = 1;
    public FunctionScheduler() {
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

    @Override

    public ContainerVm findVmForContainer(Container container) {
        ServerlessInvoker selectedVm = null;
        boolean vmSelected = false;
        switch (Constants.vmSelectionAlgo) {
            /** Selecting Vm using RR method **/
            case "RR":
                for (int x = selectedVmIndex; x <= getContainerVmList().size(); x++) {
                    ServerlessInvoker tempSelectedVm = (ServerlessInvoker) (ContainerVmList.getById(getContainerVmList(), x));
                    if (tempSelectedVm.isSuitableForContainer(container)) {
                        selectedVm = tempSelectedVm;
                        vmSelected = true;
                        break;
                    }
//                    if (cloudlet.getReschedule()) {
//                        if (tempSelectedVm == (ServerlessInvoker) (ContainerVmList.getById(getContainerVmList(), cloudlet.getVmId()))) {
//                            continue;
//                        }
//                    }

                }
                if (!vmSelected) {
                    for (int x = 1; x < selectedVmIndex; x++) {
                        ServerlessInvoker tempSelectedVm = (ServerlessInvoker) (ContainerVmList.getById(getContainerVmList(), x));
                        if (tempSelectedVm.isSuitableForContainer(container)) {
                            selectedVm = tempSelectedVm;
                            vmSelected = true;
                            break;
                        }
//                        if (cloudlet.getReschedule()) {
//                            if (tempSelectedVm == (ServerlessInvoker) (ContainerVmList.getById(getVmsCreatedList(), cloudlet.getVmId()))) {
//                                continue;
//                            }
//                        }
                    }
                }
                System.out.println(CloudSim.clock() + " >>>>>>>>>>>>>>Debug:Broker: Selected VM is # " + selectedVmIndex + " for container # " + container.getId() + " under RR method");


//                if (reschedule == false) {
//                    cloudlet.setContainerId(containerId);
//                    toSubmitOnContainerCreation.add(cloudlet);
//                }
//                if (cloudlet.getReschedule()) {
//                    tasksToReschedule.put(cloudlet, containerId);
//                    // System.out.println(CloudSim.clock() + " Debug:Broker: Need to create a new container# " + containerId + " to reschedule cloudlet # " + cl.getCloudletId());
//
//                }

                if (selectedVmIndex == getContainerVmList().size()) {
                    selectedVmIndex = 1;
                } else
                    selectedVmIndex++;

                /** Selecting Vm using Random method **/
            case "RM":
                Random random = new Random();
                while(!vmSelected) {
                    int vmNo = random.nextInt(getContainerVmList().size() - 1);
                    ServerlessInvoker tempSelectedVm = (ServerlessInvoker) (ContainerVmList.getById(getContainerVmList(), vmNo));
                    if (tempSelectedVm.isSuitableForContainer(container)) {
                        selectedVm = tempSelectedVm;
                        vmSelected = true;
                        break;
                    }
//                    if (cloudlet.getReschedule()) {
//                        if (tempSelectedVm == (ServerlessInvoker) (ContainerVmList.getById(getVmsCreatedList(), cloudlet.getVmId()))) {
//                            continue;
//                        }
//                    }
                }

                System.out.println(CloudSim.clock() + " >>>>>>>>>>>>>>Debug:Broker: Selected VM is # " + selectedVmIndex + " for container # " + container.getId() + " under RR method");

//                if (cloudlet.getReschedule()) {
//                    tasksToReschedule.put(cloudlet, containerId);
//                    // System.out.println(CloudSim.clock() + " Debug:Broker: Need to create a new container# " + containerId + " to reschedule cloudlet # " + cl.getCloudletId());
//
//                }
        }

        return selectedVm;

    }

    public boolean reallocateVmResourcesForContainer(Container container, ServerlessInvoker vm, ServerlessTasks cl) {
        boolean result = vm.reallocateResourcesForContainer(container, cl);

        return result;

    }
}