/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.util;

/**
 *
 * @author ashwinkayyoor
 */
import java.util.*;


/**
 * An {@code Iterator} implemntation that combines multiple iterators into a
 * single instance.<p>
 *
 * This class is thread-safe.
 *
 * @author David Jurgens
 */ 
public class CombinedIterator<T> implements Iterator<T> {

    /**
     * The iterators to use
     */
    private final Queue<Iterator<T>> iters;

    /**
     * The curruent iterator from which elements are being drawn.
     */
    private Iterator<T> current;

    /**
     * The iterator that was last used to return an element.  This field is
     * needed to support {@code remove}
     */
    private Iterator<T> toRemoveFrom;

    /**
     * Constructs a {@code CombinedIterator} from all of the provided iterators.
     */
    public CombinedIterator(Iterator<T>... iterators) {
	this(Arrays.asList(iterators));
    }

    /**
     * Constructs a {@code CombinedIterator} from all of the iterators in the
     * provided collection.
     */
    public CombinedIterator(Collection<Iterator<T>> iterators) {
        this((Queue<Iterator<T>>)(new ArrayDeque<Iterator<T>>(iterators)));
    }

    /**
     * Constructs a {@code CombinedIterator} from all of the iterators in the
     * provided collection.
     */
    private CombinedIterator(Queue<Iterator<T>> iterators) {
	this.iters = iterators;
        advance();
    }

    /**
     * Joins the iterators of all the provided iterables as one unified
     * iterator.
     */
    public static <T> Iterator<T> join(Collection<Iterable<T>> iterables) {
        Queue<Iterator<T>> iters = 
            new ArrayDeque<Iterator<T>>(iterables.size());
        for (Iterable<T> i : iterables)
            iters.add(i.iterator());
        return new CombinedIterator<T>(iters);
    }

    /**
     * Moves to the next iterator in the queue if the current iterator is out of
     * elements.
     */
    private void advance() {
        if (current == null || !current.hasNext()) {
            do {
                current = iters.poll();
            } while (current != null && !current.hasNext());
        }
    }

    /**
     * Returns true if there are still elements in at least one of the backing
     * iterators.
     */
    @Override
    public synchronized boolean hasNext() {
	return current != null && current.hasNext();
    }

    /**
     * Returns the next element from some iterator.
     */
    @Override
    public synchronized T next() {
	if (current == null) {
	    throw new NoSuchElementException();
	}
	T t = current.next();
        // Once an element has been drawn from the iterator, the current
        // iterator should be used for any subsequent remove call.
        if (toRemoveFrom != current)
            toRemoveFrom = current;
	advance();
	return t;
    }

    /**
     * Removes the previously returned element using the backing iterator's
     * {@code remove} method.
     */
    @Override
    public synchronized void remove() {
	if (toRemoveFrom == null) {
	    throw new IllegalStateException();
	}	
	toRemoveFrom.remove();
    }
}