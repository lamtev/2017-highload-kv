package ru.mail.polis.lamtev;

import org.jetbrains.annotations.NotNull;

import java.util.*;

final class ClusterImpl implements Cluster {

    @NotNull
    private final List<String> topology;

    ClusterImpl(@NotNull final Set<String> topology) {
        this.topology = new ArrayList<>(topology);
        this.topology.sort(String::compareTo);
    }

    @NotNull
    @Override
    public List<String> nodes(@NotNull final String id, int from) {
        final List<String> nodes = new ArrayList<>(from);
        for (int i = 0; i < from; ++i) {
            final int hash = id.hashCode() + i;
            nodes.add(topology.get(hash % topology.size()));
        }
        return nodes;
    }

    @Override
    public int numberOfNodes() {
        return topology.size();
    }

    @Override
    public int quorum() {
        return numberOfNodes() / 2 + 1;
    }

}
