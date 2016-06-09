package org.kairosdb.core.telnet;

import java.io.IOException;
import java.net.ServerSocket;

public class SocketUtil {

    public static int findFreePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
        }
        return -1;
    }

}
