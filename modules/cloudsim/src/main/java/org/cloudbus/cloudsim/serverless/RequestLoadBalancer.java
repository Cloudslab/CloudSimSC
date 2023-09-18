package org.cloudbus.cloudsim.serverless;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.lists.ContainerVmList;
import org.cloudbus.cloudsim.core.CloudSim;

import java.util.List;


/**
 * Loadbalancer class for CloudSimSC extension.
 *
 * @author Anupama Mampage
 * Created on 6/25/2023
 */
public class RequestLoadBalancer {

    /**
     * The broker ID.
     */
    private ServerlessController broker;

    /**
     * The DC.
     */
    private ServerlessDatacenter DC;
    public RequestLoadBalancer(ServerlessController controller, ServerlessDatacenter dc){
        setBroker(controller);
        setServerlessDatacenter(dc);
    }

    public void setBroker(ServerlessController br) {
        this.broker = br;
    }

    public ServerlessController getBroker() {
        return broker;
    }

    public void setServerlessDatacenter(ServerlessDatacenter dc) {
        this.DC = dc;
    }

    public ServerlessDatacenter getServerlessDatacenter() {
        return DC;
    }

    public void routeRequest(ServerlessRequest request){
        if (request.retry > Constants.MAX_RESCHEDULE_TRIES){
            broker.getCloudletList().remove(request);
            request.setSuccess(false);
            broker.getCloudletReceivedList().add(request);
        }
        else if (Constants.SCALE_PER_REQUEST){
            broker.toSubmitOnContainerCreation.add(request);
            broker.createContainer(request, request.getRequestFunctionId(), request.getUserId());
            broker.requestSubmitClock = CloudSim.clock();
        }
        else{
            boolean containerSelected = selectContainer(request);
            if (!containerSelected) {
                broker.getCloudletList().remove(request);

            }
        }

    }

    protected boolean selectContainer(ServerlessRequest task){
//        boolean containerSelected = false ;
        boolean contTypeExists = false;
        switch (Constants.CONTAINER_SELECTION_ALGO) {
            /** Selecting container using FF method **/
            case "FF": {
                for (int x = 1; x <= broker.getVmsCreatedList().size(); x++) {
                    ServerlessInvoker vm = (ServerlessInvoker) (ContainerVmList.getById(broker.getVmsCreatedList(), x));
                    assert vm != null;
                    if (vm.getFunctionContainerMap().containsKey(task.getRequestFunctionId())) {
                        contTypeExists = true;
                        List<Container> contList = vm.getFunctionContainerMap().get(task.getRequestFunctionId());
                        int y = 0;
                        for (Container container : contList) {
                            ServerlessContainer cont = (ServerlessContainer) (container);
                            ServerlessRequestScheduler clScheduler = (ServerlessRequestScheduler) (container.getContainerCloudletScheduler());
                            if (clScheduler.isSuitableForRequest(task, cont)) {
                                clScheduler.setTotalCurrentAllocatedRamForRequests(task);
                                clScheduler.setTotalCurrentAllocatedMipsShareForRequests(task);
                                Log.printLine(String.format("Using idling container: container #%s", cont.getId()));

                                task.setContainerId(cont.getId());
                                broker.addToVmTaskMap(task, vm);
                                cont.setRunningTask(task);
                                cont.setIdling(false);
                                cont.setIdleStartTime(0);
//                                if(DC.getContainersToDestroy().contains(cont)){
//                                    ((ServerlessContainer)DC.getContainersToDestroy().get(y)).setIdleStartTime(0);
//                                }
                                broker.setFunctionVmMap(vm, task.getRequestFunctionId());
                                broker.requestSubmitClock = CloudSim.clock();
                                broker.submitRequestToDC(task, vm.getId(), 0, cont.getId());
                                return true;

                            }
                            y++;

                        }

                    }

                }
                break;
            }
        }
        if(Constants.CONTAINER_CONCURRENCY && Constants.FUNCTION_HORIZONTAL_AUTOSCALING){
            if (contTypeExists){
                broker.sendFunctionRetryRequest(task);
                Log.printLine(String.format("clock %s Container type exists so rescheduling", CloudSim.clock()));

                task.retry++;
                return false;
            }
            for (int x = 1; x <= broker.getVmsCreatedList().size(); x++) {
                ServerlessInvoker vm = (ServerlessInvoker) (ContainerVmList.getById(broker.getVmsCreatedList(), x));
                assert vm != null;
                if (vm.getFunctionContainerMapPending().containsKey(task.getRequestFunctionId())) {
                    Log.printLine(String.format("clock %s Pending Container of type exists so rescheduling", CloudSim.clock()));

                    broker.sendFunctionRetryRequest(task);
                    task.retry++;
                    return false;
                }

            }
            Log.printLine(String.format("clock %s Container type does not exist so creating new", CloudSim.clock()));

            broker.createContainer(task, task.getRequestFunctionId(), task.getUserId());
            broker.sendFunctionRetryRequest(task);
            task.retry++;

            return false;
        }
        else {
            broker.toSubmitOnContainerCreation.add(task);
            broker.createContainer(task, task.getRequestFunctionId(), task.getUserId());
            broker.requestSubmitClock = CloudSim.clock();

            return true;
        }
    }

}
