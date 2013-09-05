/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.util;

/**
 *
 * @author ashwinkayyoor
 */
import java.io.Serializable;
import java.util.*;

/**
 * A Red-Black tree {@link SortedMultiMap} implementation.  Map elements are
 * sorted according to the {@link Comparable natural ordering} of the keys, or
 * by a {@link Comparator} provided to the constructor.<p>
 *
 * This implementation provides guaranteed log(n) time cost for the {@code
 * containsKey}, {@code get}, {@code put} and {@code remove} operations.  The
 * remaining operations run in constant time.  The only exception is the {@code
 * range} method, which runs in constant time except for the first time it is
 * invoked on any sub-map returned by {@code headMap}, {@code tailMap}, or
 * {@code subMap}, when it runs in linear time.<p>
 *
 * This map is not thread-safe.
 * 
 * @see Map
 * @see SortedMap
 * @see HashMultiMap
 * @see MultiMap
 */
public class TreeMultiMap<K,V> 
        implements SortedMultiMap<K,V>, Serializable {

    private static final long serialVersionUID = 1;

    /**
     * The backing map instance
     */
    private final SortedMap<K,Set<V>> map;

    /**
     * The number of values mapped to keys
     */
    private int range;
    
    /**
     * Whether the current range needs to be recalcuated before the next {@link
     * #range()} call can return the correct result.  An invalid range is the
     * result of a submap being created.
     */
    private boolean recalculateRange;

    /**
     * The super-map of this instance if it is a sub map, or {@code null} if
     * this is the original map.
     */
    private final TreeMultiMap<K,V> parent;

    /**
     * Constructs this map using the natural ordering of the keys.
     */
    public TreeMultiMap() {
	map = new TreeMap<K,Set<V>>();
	range = 0;
	recalculateRange = false;
	parent = null;
    }

    /**
     * Constructs this map where keys will be sorted according to the provided
     * comparator.
     */
    public TreeMultiMap(Comparator<? super K> c) {
	map = new TreeMap<K,Set<V>>(c);
	range = 0;
	recalculateRange = false;
	parent = null;
    }

    /**
     * Constructs this map using the natural ordering of the keys, adding all of
     * the provided mappings to this map.
     */
    public TreeMultiMap(Map<? extends K,? extends V> m) {
	this();
	putAll(m);
    }

    /**
     * Constructs a subset of an existing map using the provided map as the
     * backing map.
     *
     * @see #headMap(Object)
     * @see #subMap(Object,Object)
     * @see #tailMap(Object)
     */
    private TreeMultiMap(SortedMap<K,Set<V>> subMap, TreeMultiMap<K,V> parent) {
	map = subMap;
	// need to compute the range, but lazily compute this on demand.
	// Calculuating it now turns the subMap operations into O(n) instead of
	// the O(1) call it currently is.
	range = -1;
	recalculateRange = true;
	this.parent = parent;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<K,Set<V>> asMap() {
        // REMINDER: map should be wrapped with a class to prevent mapping keys
        // to null
        return map;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clear() {
	map.clear();
	if (parent != null) {
	    int curRange = range();
	    parent.updateParentRange(-curRange);
	}
	range = 0;
    }

    /**
     * Recursively updates the range value for the the super-map of this sub-map
     * if it exists.
     */
    private void updateParentRange(int valueDifference) {
	range += valueDifference;
	if (parent != null) {
	    parent.updateParentRange(valueDifference);
	}
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Comparator<? super K> comparator() {
	return map.comparator();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsKey(Object key) {
	return map.containsKey(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsMapping(Object key, Object value) {
        Set<V> s = map.get(key);
        return s != null && s.contains(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsValue(Object value) {
	for (Set<V> s : map.values()) {
	    if (s.contains(value)) {
		return true;
	    }
	}
	return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<Map.Entry<K,V>> entrySet() {
	return new EntryView();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public K firstKey() {
	return map.firstKey();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<V> get(Object key) {
	return map.get(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SortedMultiMap<K,V> headMap(K toKey) {
	return new TreeMultiMap<K,V>(map.headMap(toKey), this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isEmpty() {
	return map.isEmpty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<K> keySet() {
	return map.keySet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public K lastKey() {
	return map.lastKey();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean put(K key, V value) {
	Set<V> values = map.get(key);
	if (values == null) {
	    values = new HashSet<V>();
	    map.put(key, values);
	}
	boolean added = values.add(value);
	if (added) {
	    range++;
	    if (parent != null) {
		parent.updateParentRange(1);
	    }
	}
	return added;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void putAll(Map<? extends K,? extends V> m) {
	for (Map.Entry<? extends K,? extends V> e : m.entrySet()) {
	    put(e.getKey(), e.getValue());
	}
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void putAll(MultiMap<? extends K,? extends V> m) {
        for (Map.Entry<? extends K,? extends V> e : m.entrySet()) {
            put(e.getKey(), e.getValue());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean putMany(K key, Collection<V> values) {
        // Short circuit when adding empty values to avoid adding a key with an
        // empty mapping
        if (values.isEmpty())
            return false;
	Set<V> vals = map.get(key);
	if (vals == null) {
	    vals = new HashSet<V>();
	    map.put(key, vals);
	}
	int oldSize = vals.size();
	boolean added = vals.addAll(values);
	range += (vals.size() - oldSize);
	return added;
    }

    /**
     * {@inheritDoc} This method runs in constant time, except for the first
     * time called on a sub-map, when it runs in linear time to the number of
     * values.
     */
    @Override
    public int range() {
	// the current range isn't accurate, loop through the values and count
	if (recalculateRange) {
	    recalculateRange = false;
	    range = 0;
	    for (V v : values()) 
		range++;
	}
	return range;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<V> remove(Object key) {
	Set<V> v = map.remove(key);
	if (v != null)
	    range -= v.size();
	if (parent != null) {
	    parent.updateParentRange(-(v.size()));
	}

	return v;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean remove(Object key, Object value) {
	Set<V> values = map.get(key);
        // If the key was not mapped to any values
        if (values == null)
            return false;
	boolean removed = values.remove(value);
	if (removed) {
	    range--;
	    if (parent != null) {
		parent.updateParentRange(-1);
	    }
	}
	// if this was the last value mapping for this key, remove the
	// key altogether
	if (values.size() == 0)
	    map.remove(key);
	return removed;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int size() {
	return map.size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SortedMultiMap<K,V> subMap(K fromKey, K toKey) {
	return new TreeMultiMap<K,V>(map.subMap(fromKey, toKey), this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SortedMultiMap<K,V> tailMap(K fromKey) {
	return new TreeMultiMap<K,V>(map.tailMap(fromKey), this);
    }

    /**
     * Returns the string form of this multi-map
     */
    @Override
    public String toString() {
	Iterator<Map.Entry<K,Set<V>>> it = map.entrySet().iterator();
	if (!it.hasNext()) {
	    return "{}";
	}
   
	StringBuilder sb = new StringBuilder();
	sb.append('{');
	while (true) {
	    Map.Entry<K,Set<V>> e = it.next();
	    K key = e.getKey();
	    Set<V> values = e.getValue();
	    sb.append(key   == this ? "(this Map)" : key);
	    sb.append("=[");
	    Iterator<V> it2 = values.iterator();
	    while (it2.hasNext()) {
		V value = it2.next();
		sb.append(value == this ? "(this Map)" : value);
		if (it2.hasNext()) {
		    sb.append(",");
		}
	    }
	    sb.append("]");
	    if (!it.hasNext())
		return sb.append('}').toString();
	    sb.append(", ");
	}
    }

    /**
     * {@inheritDoc} The collection and its {@code Iterator} are backed by the
     * map, so changes to the map are reflected in the collection, and
     * vice-versa.
     */
    @Override
    public Collection<V> values() {
	return new ValuesView();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<Set<V>> valueSets() {
        return map.values();
    }

    /**
     * A {@link Collection} view of the values contained in a {@link MultiMap}.
     *
     * @see MultiMap#values()
     */
    class ValuesView extends AbstractCollection<V> implements Serializable {

	private static final long serialVersionUID = 1;

	public ValuesView() { }

	/**
	 * {@inheritDoc}
	 */
        @Override
	public void clear() {
	    map.clear();
	}

	/**
	 * {@inheritDoc}
	 */
        @Override
	public boolean contains(Object o) {
	    return containsValue(o);
	}

	/**
	 * {@inheritDoc}
	 */
        @Override
	public Iterator<V> iterator() {
	    // combine all of the iterators for the entry sets
	    Collection<Iterator<V>> iterators = 
		new ArrayList<Iterator<V>>(size());
	    for (Set<V> s : map.values()) {
		iterators.add(s.iterator());
	    }
	    // NOTE: because the iterators are backed by the internal map and
	    // because the CombinedIterator class supports remove() if the
	    // backing class does, calls to remove from this iterator are also
	    // supported.
	    return new CombinedIterator<V>(iterators);
	}

	/**
	 * {@inheritDoc}
	 */
	public int size() {
	    return range();
	}
    }    

    /**
     * A {@link Collection} view of the values contained in a {@link MultiMap}.
     *
     * @see MultiMap#values()
     */
    class EntryView extends AbstractSet<Map.Entry<K,V>> 
	    implements Serializable {

	private static final long serialVersionUID = 1;

	public EntryView() { }

	/**
	 * {@inheritDoc}
	 */
	public void clear() {
	    map.clear();
	}

	/**
	 * {@inheritDoc}
	 */
	public boolean contains(Object o) {
	    if (o instanceof Map.Entry) {
		Map.Entry<?,?> e = (Map.Entry<?,?>)o;
		Set<V> vals = TreeMultiMap.this.get(e.getKey());
		return vals.contains(e.getValue());
	    }
	    return false;
	}

	/**
	 * {@inheritDoc}
	 */
	public Iterator<Map.Entry<K,V>> iterator() {
	    return new EntryIterator();
	}

	/**
	 * {@inheritDoc}
	 */
	public int size() {
	    return range();
	}
    }

    /**
     * An iterator of all the key-value mappings in the multi-map.
     */
    class EntryIterator implements Iterator<Map.Entry<K,V>>, Serializable {

        private static final long serialVersionUID = 1;

	K curKey;
	Iterator<V> curValues;
	Iterator<Map.Entry<K,Set<V>>> multiMapIterator;

	Map.Entry<K,V> next;
	Map.Entry<K,V> previous;

	public EntryIterator() {
	    multiMapIterator = map.entrySet().iterator();
	    if (multiMapIterator.hasNext()) {
		Map.Entry<K,Set<V>> e = multiMapIterator.next();
		curKey =  e.getKey();
		curValues = e.getValue().iterator();
	    }

	    advance();
	}

	private void advance() {
	    // Check whether the current key has any additional mappings that
	    // have not been returned
	    if (curValues != null && curValues.hasNext()) {
		next = new MultiMapEntry(curKey, curValues.next());
		//System.out.println("next = " + next);
	    }
	    else if (multiMapIterator.hasNext()) {
		Map.Entry<K,Set<V>> e = multiMapIterator.next();
		curKey =  e.getKey();
		curValues = e.getValue().iterator();
		// Assume that the map correct manages the keys and values such
		// that no key is ever mapped to an empty set
		next = new MultiMapEntry(curKey, curValues.next());
	    } else {
		next = null;		
	    }
	}

	public boolean hasNext() {
	    return next != null;
	}

	public Map.Entry<K,V> next() {
	    Map.Entry<K,V> e = next;
	    previous = e;
	    advance();
	    return e;
	}

	public void remove() {
	    if (previous == null) {
                throw new IllegalStateException(
                    "No previous element to remove");
	    }
	    TreeMultiMap.this.remove(previous.getKey(), previous.getValue());
	    previous = null;
	}

	/**
	 * A {@link Map.Entry} implementation that handles {@link MultiMap}
	 * semantics for {@code setValue}.
	 */
	private class MultiMapEntry extends AbstractMap.SimpleEntry<K,V> 
	        implements Serializable {

	    private static final long serialVersionUID = 1;

	    public MultiMapEntry(K key, V value) {
		super(key, value);
	    }

	    public V setValue(V value) {
		Set<V> values = TreeMultiMap.this.get(getKey());
		values.remove(getValue());
		values.add(value);
		return super.setValue(value);
	    }
	}
    }




}