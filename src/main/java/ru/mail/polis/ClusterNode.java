package ru.mail.polis;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

public final class ClusterNode {

    private ClusterNode() {
        throw new UnsupportedOperationException("Not for instance");
    }

    public static void main(String[] args) {
        final int nArgs = args.length;
        switch (nArgs) {
            case 1:
            case 7:
                break;
            default:
                unexpectedArguments();
                help();
                return;
        }
        switch (args[0].toLowerCase()) {
            case "start":
                try {
                    start(args);
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                    unexpectedArguments();
                    help();
                }
                break;
            case "help":
            default:
                help();
        }
    }

    private static void start(String[] args) throws IOException {
        final int port = parsePort(args);
        final File directory = parseDirectory(args);
        final Set<String> topology = parseTopology(args);

        final KVService node = KVServiceFactory.create(port, directory, topology);
        node.start();
        Runtime.getRuntime().addShutdownHook(new Thread(node::stop));

        System.out.println("Starting node on port " + port + " and data at " + directory.getAbsolutePath());
        System.out.println("Topology: " + topology);
    }

    private static int parsePort(String[] args) {
        switch (args[1].toLowerCase()) {
            case "-p":
                return Integer.valueOf(args[2]);
            default:
                throw new IllegalArgumentException();
        }
    }

    private static File parseDirectory(String[] args) {
        switch (args[3].toLowerCase()) {
            case "-d":
                return new File(args[4]);
            default:
                throw new IllegalArgumentException();
        }
    }

    private static Set<String> parseTopology(String[] args) {
        switch (args[5].toLowerCase()) {
            case "-t":
                final String nodeAddrs = args[6];
                final int length = nodeAddrs.length();
                final char first = nodeAddrs.charAt(0);
                final char last = nodeAddrs.charAt(length - 1);
                if (first != '[' || last != ']') {
                    throw new IllegalArgumentException();
                }
                return Arrays.stream(nodeAddrs.substring(1, length-1).split("\\s*(,)\\s*"))
                        .map(it -> "http://" + it)
                        .collect(toSet());
            default:
                throw new IllegalArgumentException();
        }
    }

    private static void help() {
        System.out.println("start -p <PORT> -d <DIRECTORY> -t <TOPOLOGY>\n" +
                "Where:\n" +
                "PORT      -- number in range [1, 65535] -- port number\n" +
                "DIRECTORY -- path to directory to store data\n" +
                "TOPOLOGY  -- ip address of each topology node in format:\n" +
                "[x.x.x.x:zz,x.x.x.x:zz,...] (Note it shouldn't contain whitespaces)");
    }

    private static void unexpectedArguments() {
        System.out.println("Unexpected arguments!!!\n" + "See help.");
    }

}
