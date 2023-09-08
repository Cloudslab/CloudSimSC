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


        /*if (ram >= maxRam) {
            ram = maxRam;
        }*/

        deallocateRamForContainer(container);
        System.out.println("Debug: Before: Ramprovisioner: Available ram : "+getAvailableVmRam()+" Requested ram: "+ram);



        if (getAvailableVmRam() >= ram) {
            System.out.println("New available ram: "+(getAvailableVmRam() - ram));
            setAvailableVmRam(getAvailableVmRam() - ram);
            getContainerRamTable().put(container.getUid(), ram);
            container.setCurrentAllocatedRam(ram);
            System.out.println("Debug: After: Ramprovisioner: Available ram : "+getAvailableVmRam()+" Requested ram: "+ram);
            return true;
        }



        container.setCurrentAllocatedRam(oldRam);

        return false;
    }




}
