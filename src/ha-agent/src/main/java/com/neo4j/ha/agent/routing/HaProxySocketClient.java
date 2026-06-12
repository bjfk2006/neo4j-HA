package com.neo4j.ha.agent.routing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

public class HaProxySocketClient {

    private static final Logger log = LoggerFactory.getLogger(HaProxySocketClient.class);

    public String sendCommand(String socketPath, String command) throws IOException {
        UnixDomainSocketAddress address = UnixDomainSocketAddress.of(socketPath);
        try (SocketChannel channel = SocketChannel.open(StandardProtocolFamily.UNIX)) {
            channel.connect(address);

            String fullCommand = command + "\n";
            channel.write(ByteBuffer.wrap(fullCommand.getBytes(StandardCharsets.UTF_8)));

            ByteBuffer buffer = ByteBuffer.allocate(4096);
            StringBuilder response = new StringBuilder();
            int bytesRead;
            while ((bytesRead = channel.read(buffer)) > 0) {
                buffer.flip();
                response.append(StandardCharsets.UTF_8.decode(buffer));
                buffer.clear();
            }

            return response.toString().trim();
        }
    }
}
