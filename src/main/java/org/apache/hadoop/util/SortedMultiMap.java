/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.util;

/**
 *
 * @author ashwinkayyoor
 */
import java.util.Comparator;
import java.util.Set;

/**
 * A {@link MultiMap} that provides a total ordering for the keys.  All keys
 * intersted must implement the {@link Comparable} interface.  
 *
 * @see MultiMap
 * @see java.util.SortedMap
 */
public interface SortedMultiMap<K,V> extends MultiMap<K,V> {

    /**
     * Returns the comparator used to order the keys in this map, or null if
     * this map uses the {@link Comparable natural ordering} of its keys.
     */
    Comparator<? super K> comparator();

    /**
     * Returns a {@link Set} view of the mappings contained in this map.
     */
    K firstKey();

    /**
     * Returns a view of the portion of this map whose keys are less than {@code
     * toKey}.
     */
    SortedMultiMap<K,V> headMap(K toKey);

    /**
     * Returns the last (highest) key currently in this map.
     */
    K lastKey();

    /**
     * Returns a view of the portion of this map whose keys range from {@code
     * fromKey}, inclusive, to {@code toKey}, exclusive.
     */
    SortedMultiMap<K,V> subMap(K fromKey, K toKey);

    /**
     * Returns a view of the portion of this map whose keys are greater than or
     * equal to {@code fromKey}.
     */
    SortedMultiMap<K,V> tailMap(K fromKey);

}