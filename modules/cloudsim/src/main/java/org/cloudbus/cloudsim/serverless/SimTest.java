package org.cloudbus.cloudsim.serverless;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;
import java.util.Calendar;

public class SimTest {


    public static void main(String[] args) {
        Log.printLine("Starting SimTest1...");

        try {
            /**
             * number of cloud Users
             */
            int num_user = 1;
            /**
             *  The fields of calender have been initialized with the current date and time.
             */
            Calendar calendar = Calendar.getInstance();
            /**
             * Deactivating the event tracing
             */
            boolean trace_flag = false;
            /**
             * 1- Like CloudSim the first step is initializing the CloudSim Package before creating any entities.
             *
             */
            CloudSim.init(num_user, calendar, trace_flag);
            int overBookingFactor = 80;
            ServerlessController controller = createBroker(overBookingFactor);
            int controllerId = controller.getId();
            controller.start();


        } catch (Exception e) {
            e.printStackTrace();
            Log.printLine("Unwanted errors happen");

        }
    }

    private static ServerlessController createBroker(int overBookingFactor) {

        ServerlessController controller = null;

        try {
            controller = new ServerlessController("Broker", overBookingFactor);
        } catch (Exception var2) {
            var2.printStackTrace();
            System.exit(0);
        }

        return controller;
    }
}
