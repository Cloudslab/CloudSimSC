package org.cloudbus.cloudsim.serverless;

import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.core.ContainerDatacenterBroker;
import org.cloudbus.cloudsim.container.core.ContainerHost;

import java.util.*;

/**
 * Broker class for CloudSimServerless extension. This class represents a broker (Service Provider)
 * who uses the Cloud data center.
 *
 * @author Anupama Mampage
 */

public class ServerlessController extends ContainerDatacenterBroker {

    private static List<ContainerHost> hostList = new ArrayList<ContainerHost>();
    private static List<ServerlessInvoker> vmList = new ArrayList<ServerlessInvoker>();
    private static List<ServerlessInvoker> vmIdleList = new ArrayList<>();
    private static List<Container> containerList = new ArrayList<Container>();
    private static List<ServerlessTasks> cloudletList = new ArrayList<ServerlessTasks>();
    private Queue<Double> cloudletArrivalTime = new LinkedList<Double>();
    private Map<String, ArrayList<ServerlessInvoker>> functionVmMap = new HashMap<String, ArrayList<ServerlessInvoker>>();
    private Queue cloudletQueue = new LinkedList<ServerlessTasks>();
    private List<ServerlessTasks> toSubmitOnContainerCreation = new ArrayList<ServerlessTasks>();
    private List<Double> averageVmUsageRecords = new ArrayList<Double>();
    private List<Double> meanAverageVmUsageRecords = new ArrayList<Double>();
    private List<Integer> vmCountList = new ArrayList<Integer>();
    private List<Double> meanSumOfVmCount = new ArrayList<Double>();
    private double timeInterval = 50.0;
    private Map<ServerlessInvoker, ArrayList<ServerlessTasks>> vmTempTimeMap = new HashMap<ServerlessInvoker,ArrayList<ServerlessTasks>>();
    public int controllerId=0;
    public int overBookingfactor = 0;
    public static int containerId = 1;
    ServerlessDatacenter e ;
    private boolean reschedule = false;
    int dcount = 1;
    int exsitingContCount = 0;
    public int noOfTasks = 0;
    public int noOfTasksReturned = 0;
    private String vmSelectionMode= "RR";
    private String subMode= "NEWBM";
    /** Vm indexfor selecting Vm in round robin fashion **/
    private int selectedVmIndex = 1;
    /** The map of tasks to reschedule with the new containerId. */
    private static Map<ServerlessTasks, Integer> tasksToReschedule = new HashMap<>();

    public ServerlessController(String name, double overBookingfactor) throws Exception {
        super(name, overBookingfactor);
        this.overBookingfactor = (int) overBookingfactor;
    }
}
