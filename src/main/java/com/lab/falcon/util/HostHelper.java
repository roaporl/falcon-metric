package com.lab.falcon.util;

import java.io.IOException;
import java.net.*;
import java.util.Collections;
import java.util.Random;

/**
 * Created by Aporl on 2014/11/28.
 */
public class HostHelper {

    public static int getAvailablePort() {
        int port = new Random().nextInt(10000) + 10000;
        while (!isPortAvailable(port)) {
            port = new Random().nextInt(10000) + 10000;
        }
        return port;
    }

    public static boolean isPortAvailable(int port) {
        try {
            ServerSocket server = new ServerSocket(port);
            server.close();
            return true;
        } catch (IOException ignore) {
        }
        return false;
    }

    public static String getLocalHost() {
        return InetAddressHolder.inetAddress.getHostName();
    }

    public static String getLocalHostIp() {
        return InetAddressHolder.inetAddress.getHostAddress();
    }

    private static InetAddress holdLocalHostInetAddress() {
        try {
            for (NetworkInterface iface : Collections.list(NetworkInterface.getNetworkInterfaces())) {
                if (!iface.getName().startsWith("vmnet") && !iface.getName().startsWith("docker")) {
                    for (InetAddress raddr : Collections.list(iface.getInetAddresses())) {
                        if (raddr.isSiteLocalAddress() && !raddr.isLoopbackAddress() && !(raddr instanceof Inet6Address)) {
                            return raddr;
                        }
                    }
                }
            }
        } catch (SocketException ignore) {
        }
        throw new IllegalStateException("Couldn't find the local machine ip.");
    }

    public static InetAddress getLocalHostInetAddress() {
        return InetAddressHolder.inetAddress;
    }


    public static class InetAddressHolder {

        static InetAddress inetAddress = holdLocalHostInetAddress();
    }
}
