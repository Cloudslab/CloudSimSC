package org.cloudbus.cloudsim.serverless;

import org.cloudbus.cloudsim.container.containerProvisioners.ContainerRamProvisionerSimple;
import org.cloudbus.cloudsim.container.core.Container;

public class ServerlessContainerRamProvisioner extends ContainerRamProvisionerSimple {
    public ServerlessContainerRamProvisioner(float availableRam) {
        super(availableRam);

    }

    @Override
    public boolean allocateRamForContainer(Container container,
                                           float ram) {
        float oldRam = container.getCurrentAllocatedRam();
        deallocateRamForContainer(container);

        if (getAvailableVmRam() >= ram) {
            System.out.println("New available ram: "+(getAvailableVmRam() - ram));
            setAvailableVmRam(getAvailableVmRam() - ram);
            getContainerRamTable().put(container.getUid(), ram);
            container.setCurrentAllocatedRam(ram);
            return true;
        }



        container.setCurrentAllocatedRam(oldRam);

        return false;
    }




}
