package org.cloudbus.cloudsim.serverless;

import org.cloudbus.cloudsim.container.containerProvisioners.ContainerPe;
import org.cloudbus.cloudsim.container.schedulers.ContainerSchedulerTimeSharedOverSubscription;

import java.util.List;

public class ServerlessContainerScheduler extends ContainerSchedulerTimeSharedOverSubscription {
    /**
     * Instantiates a new container scheduler time shared.
     *
     * @param pelist the pelist
     */
    public ServerlessContainerScheduler(List<? extends ContainerPe> pelist) {
        super(pelist);
    }
}