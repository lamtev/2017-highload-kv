package ru.mail.polis.lamtev;

import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Set;

public interface Cluster {

    @NotNull
    static Cluster of(@NotNull final Set<String> topology) {
        return new ClusterImpl(topology);
    }

    @NotNull
    List<String> nodes(@NotNull final String id, int from);

    int numberOfNodes();

    int quorum();

}
