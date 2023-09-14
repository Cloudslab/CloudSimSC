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
    /**
     * Insert the policy for selecting a VM for a container
     */
    public ContainerVm findVmForContainer(Container container) {
        ServerlessInvoker selectedVm = null;
        boolean vmSelected = false;
        switch (Constants.vmSelectionAlgo) {
            /** Selecting Vm using RR method **/
            case "RR": {
                for (int x = selectedVmIndex; x <= getContainerVmList().size(); x++) {
                    ServerlessInvoker tempSelectedVm = (ServerlessInvoker) (ContainerVmList.getById(getContainerVmList(), x));
                    if (tempSelectedVm.isSuitableForContainer(container, tempSelectedVm)) {
                        selectedVm = tempSelectedVm;
                        vmSelected = true;
                        break;
                    }
//                    if (request.getReschedule()) {
//                        if (tempSelectedVm == (ServerlessInvoker) (ContainerVmList.getById(getContainerVmList(), request.getVmId()))) {
//                            continue;
//                        }
//                    }

                }

                if (!vmSelected) {
                    for (int x = 1; x < selectedVmIndex; x++) {
                        ServerlessInvoker tempSelectedVm = (ServerlessInvoker) (ContainerVmList.getById(getContainerVmList(), x));
                        if (tempSelectedVm.isSuitableForContainer(container, tempSelectedVm)) {
                            selectedVm = tempSelectedVm;
                            vmSelected = true;
                            break;
                        }
//                        if (request.getReschedule()) {
//                            if (tempSelectedVm == (ServerlessInvoker) (ContainerVmList.getById(getVmsCreatedList(), request.getVmId()))) {
//                                continue;
//                            }
//                        }
                    }
                }
                System.out.println(CloudSim.clock() + " >>>>>>>>>>>>>>Debug:Broker: Selected VM is # " + selectedVmIndex + " for container # " + container.getId() + " under RR method");


                if (selectedVmIndex == getContainerVmList().size()) {
                    selectedVmIndex = 1;
                } else
                    selectedVmIndex++;
                break;
            }

                /** Selecting Vm using Random method **/
            case "RM": {
                Random random = new Random();
                while (!vmSelected) {
                    int vmNo = random.nextInt(getContainerVmList().size() - 1);
                    ServerlessInvoker tempSelectedVm = (ServerlessInvoker) (ContainerVmList.getById(getContainerVmList(), vmNo));
                    if (tempSelectedVm.isSuitableForContainer(container, tempSelectedVm)) {
                        selectedVm = tempSelectedVm;
//                        vmSelected = true;
                        break;
                    }
                }
                break;
            }

            case "BPFF": {
                for (int x = 1; x <= getContainerVmList().size(); x++) {
                    ServerlessInvoker tempSelectedVm = (ServerlessInvoker) (ContainerVmList.getById(getContainerVmList(), x));
                    if (tempSelectedVm.isSuitableForContainer(container, tempSelectedVm)) {
                        selectedVm = tempSelectedVm;
//                        vmSelected = true;
                        break;
                    }
                }
                break;
            }



            case "BPBF": {
                double minRemainingCap = Double.MAX_VALUE;
                for (int x = 1; x <= getContainerVmList().size(); x++) {
                    ServerlessInvoker tempSelectedVm = (ServerlessInvoker) (ContainerVmList.getById(getContainerVmList(), x));
//                    System.out.println(CloudSim.clock() + " >>>>>>>>>>>>>>Debug:Broker: Vm # " + tempSelectedVm.getId() + " has available ram of " + tempSelectedVm.getContainerRamProvisioner().getAvailableVmRam() + " needed ram "+ container.getRam());

                    double vmCpuAvailability = tempSelectedVm.getAvailableMips() / tempSelectedVm.getTotalMips();
                    if (tempSelectedVm.isSuitableForContainer(container, tempSelectedVm)) {
                        if (vmCpuAvailability < minRemainingCap) {
                            selectedVm = tempSelectedVm;
                            minRemainingCap = vmCpuAvailability;
                        }


                    }
                }

            }


            System.out.println(CloudSim.clock() + " >>>>>>>>>>>>>>Debug:Broker: Selected VM is # " + selectedVm.getId() + " for container # " + container.getId() + " under "+Constants.vmSelectionAlgo+" method");

        }

        return selectedVm;

    }

    @Override
    public void deallocateVmForContainer(Container container) {
        ContainerVm containerVm = getContainerTable().remove(container.getUid());
        if (containerVm != null) {
            ((ServerlessInvoker)containerVm).containerDestroy(container);
        }
    }

    public boolean reallocateVmResourcesForContainer(Container container, ServerlessInvoker vm, int cpuChange, int memChange) {
        boolean result = vm.reallocateResourcesForContainer(container, cpuChange, memChange);

        return result;

    }
}