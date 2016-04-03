package org.etl.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;


public class DirectedAcyclicGraph<T> {

    private final Multimap<T, T> incoming = HashMultimap.create();

    private final  Multimap<T, T> outgoing = HashMultimap.create();
    /**
     * Adds a directed edge from <code>origin to target. The vertices are not
     * required to exist prior to this call - if they are not currently contained by the graph, they are
     * automatically added.
     *
     * @param source the source vertex of the dependency
     * @param target the target vertex of the dependency
     * @return <code>true if the edge was added, false if the
     *         edge was not added because it would have violated the acyclic nature of the
     *         receiver.
     */
    public boolean addEdge(T source, T target) {

        Preconditions.checkNotNull(source);
        Preconditions.checkNotNull(target);

        if (hasPath(target, source))
            return false;

        outgoing.put(source, target);
        outgoing.put(target, null);
        incoming.put(target, source);
        incoming.put(source, null);
        return true;
    }

    private boolean hasPath(T start, T end) {
        // break condition
        if (start == end)
            return true;

        Collection<T> children= outgoing.get(start);
        for (T child : children) {
            // recursion
            if (hasPath(child, end)) {
                return true;
            }
        }

        return false;
    }
    /**
     * Adds a vertex to the graph. If the vertex does not exist prior to this call, it is added with
     * no incoming or outgoing edges. Nothing happens if the vertex already exists.
     *
     * @param vertex the new vertex
     */
    public void addVertex(T vertex) {
        Preconditions.checkNotNull(vertex);
        outgoing.put(vertex, null);
        incoming.put(vertex, null);
    }

    public void removeVertex(T vertex) {
        Collection<T> targets= outgoing.removeAll(vertex);
        for (Iterator it= targets.iterator(); it.hasNext();)
            incoming.remove(it.next(), vertex);
        Collection<T> sources= incoming.removeAll(vertex);
        for (Iterator it= sources.iterator(); it.hasNext();)
            outgoing.remove(it.next(), vertex);
    }

    /**
     * Returns the sources of the receiver. A source is a vertex with no incoming edges. The
     * returned set's iterator traverses the nodes in the order they were added to the graph.
     *
     * @return the sources of the receiver
     */
    public ImmutableSet<T> getSources() {
        return computeZeroEdgeVertices(incoming);
    }

    /**
     * Returns the sinks of the receiver. A sink is a vertex with no outgoing edges. The returned
     * set's iterator traverses the nodes in the order they were added to the graph.
     *
     * @return the sinks of the receiver
     */
    public ImmutableSet<T> getSinks() {
        return computeZeroEdgeVertices(outgoing);
    }

    private ImmutableSet<T> computeZeroEdgeVertices(Multimap<T,T> map) {
        ImmutableSet.Builder<T> roots = ImmutableSet.builder();
        for (Map.Entry<T, Collection<T>> candidate : map.asMap().entrySet()) {
            Collection<T> values = candidate.getValue();
            boolean valuesIsEmpty = values.isEmpty() || (values.size() == 1 && values.iterator().next() == null);
            if (valuesIsEmpty) {
                roots.add(candidate.getKey());
            }
        }
        return roots.build();
    }

    /**
     * Returns the direct children of a vertex. The returned {@link Set} is unmodifiable.
     *
     * @param vertex the parent vertex
     * @return the direct children of <code>vertex
     */
    public ImmutableSet<T>  getChildren(T vertex) {
        ImmutableSet.Builder<T> builder = ImmutableSet.builder();
        builder.addAll(outgoing.get(vertex));
        return builder.build();
    }

    /**
     * Returns the direct parents of a vertex. The returned {@link Set} is unmodifiable.
     *
     * @param vertex the parent vertex
     * @return the direct parent of <code>vertex
     */
    public ImmutableSet<T>  getParent(T vertex) {
        ImmutableSet.Builder<T> builder = ImmutableSet.builder();
        builder.addAll(incoming.get(vertex));
        return builder.build();
    }

    public Multimap<T, T> getIncoming() {
        return incoming;
    }

    public Multimap<T, T> getOutgoing() {
        return outgoing;
    }
}
