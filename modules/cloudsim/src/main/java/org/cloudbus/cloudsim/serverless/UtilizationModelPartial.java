package org.cloudbus.cloudsim.serverless;

import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelFull;

/**
 * New utilization model class for partial usage of a resource
 *
 * @author Anupama Mampage
 */

public class UtilizationModelPartial implements UtilizationModel {
    @Override
    public double getUtilization(double time) {
        return 0;
    }

    public double getCpuUtilization(ServerlessRequest request) {
        return request.getCpuShareRequest();
    }
    public double getMemUtilization(ServerlessRequest request) {
        return request.getMemShareRequest();
    }


}
