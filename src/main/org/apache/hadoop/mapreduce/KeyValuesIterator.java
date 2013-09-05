/**
 *
 */
package org.apache.hadoop.mapreduce;

import java.util.*;
import org.apache.hadoop.mapreduce.lib.output.RecordReader;
import org.apache.hadoop.io.WritableComparable;

/**
 * This will allow for merging of multiple (sorted) inputs and presents them as
 * a single iterable stream, with the values grouped by the key
 *
 * This is used to group the outputs of the Map stage so that the values are
 * presented for reductionin the Reduce Stage.
 *
 * @author tim
 */
public class KeyValuesIterator<KEY_IN extends WritableComparable, VALUE_IN extends WritableComparable> {

    //protected static Logger log = Logger.getLogger(KeyValuesIterator.class.getName());
    // the readers being merged
    protected List<RecordReader<KEY_IN, VALUE_IN>> readers;
    // the keys of the current row for each reader
    // the index of the array maps to the readers being read
    protected List<KEY_IN> keys;
    protected PriorityQueue<KEY_IN> pqKeys;
    // cache the current key 
    public KEY_IN currentKey;
    // cache the current values
    protected Collection<VALUE_IN> currentValues;
    protected Properties prop;
    protected int arch;
    KeyComparator comparator;

    /**
     * Initialises this merged, by reading the first record from each reader
     *
     * @param readers These absolutely have to work on data that is sorted, or
     * else very strange results will ensue
     */
    public KeyValuesIterator(List<RecordReader<KEY_IN, VALUE_IN>> readers, Properties prop) {
        this.readers = readers;
        this.prop = prop;
        this.arch = Integer.parseInt(prop.getProperty("arch"));
        //comparator = new KeyComparator();

        //this.keys = new LinkedList<KEY_IN>();
        //this.currentValues = new LinkedList<VALUE_IN>();
        //Collections.copy(this.readersCopy, readers);
        //log.debug("Merging input from " + readers.size() + " readers");
    }

    /**
     * Increments the readers, assembles the next collection of values
     * [Potentially we could sort the values here, but it is not expected that
     * most implementations require or expect sorted values so this
     * implementation does not sort the values at this time]
     *
     * @return true if there are more to read, false if we are all done
     */
    public final boolean nextKeyValues() {
        KEY_IN oldCurrentKey = currentKey;

        // if this is the first entry, initialise all the readers appropriately
        if (keys == null) {
            //log.debug("Initialising the first keys");
            keys = new ArrayList<KEY_IN>();

//            keys.clear();
            for (Iterator<RecordReader<KEY_IN, VALUE_IN>> iter = readers.iterator(); iter.hasNext();) {
                RecordReader<KEY_IN, VALUE_IN> reader = iter.next();

                // if the reader is before the first record, read the first
                if (reader.getCurrentKey() == null) {
                    if (!reader.nextKeyValue()) {
                        // seems we received an empty reader
                        iter.remove();
                        //log.debug("Reader initialised with no data, so removed");
                    } else {
                        //log.debug("Reader initialised with first key: " + reader.getCurrentKey());
                        keys.add(reader.getCurrentKey());
                        // store the current key we are working on
                        if (currentKey == null || reader.getCurrentKey().compareTo(currentKey) < 0) {
                            currentKey = reader.getCurrentKey();
                        }
                    }
                }

            }
        } else { // not the first entry, so we find the lowest key in the keys
            currentKey = null;
            for (KEY_IN k : keys) {
                if (currentKey == null || k.compareTo(currentKey) <= 0) {
                    currentKey = k;
                }
            }
        }

        //log.debug("Working on key[" + currentKey + "], keys[" + keys + "], readers[" + readers.size() + "]");

        // so we now know the key we are working on, we need to read everything any reader can provide
        // with that key, and once read, move the cursor onwards.  It is important to remember that if
        // we are working on key:4, and we read all the values, the key will end up being the next key
        // and not the current key, ready for the next read
        currentValues = new ArrayList<VALUE_IN>();
        int index = 0;
        for (Iterator<RecordReader<KEY_IN, VALUE_IN>> iter = readers.iterator(); iter.hasNext();) {
            RecordReader<KEY_IN, VALUE_IN> reader = iter.next();

            // read all content for readers on the current key
            while (reader.getCurrentKey().equals(currentKey)) {
                //currentValues.add(reader.getCurrentValue());

                // move the reader on, removing if it is finished
                if (!reader.nextKeyValue()) {
                    //log.debug("Removing reader, since all content read");
                    iter.remove();
                    keys.remove(index);
                    // we remove one as the index increments each reader, so will increment below
                    index--;
                    break;
                } else {
                    keys.set(index, reader.getCurrentKey());
                }
            }

            // note the comment above
            index++;

        }

        //log.debug(keys.size() + " keys are active, the lowest being [" + currentKey + "], previous[" + oldCurrentKey + "], keys[" + keys + "]");
        //System.out.println(keys.size() + " keys are active, the lowest being [" + currentKey + "], previous["+ oldCurrentKey +"], keys[" + keys + "]");

        if (currentKey == null // no data, regardless of what came before
                || (oldCurrentKey != null && oldCurrentKey.equals(currentKey))) { // same key
            return false;
        }
        return true;
    }

    /**
     * Increments the readers, assembles the next collection of values
     * [Potentially we could sort the values here, but it is not expected that
     * most implementations require or expect sorted values so this
     * implementation does not sort the values at this time]
     *
     * @return true if there are more to read, false if we are all done
     */
    public final boolean nextKeyValues(Partitioner partitioner, int numReducers, int reducerID)  {
        KEY_IN oldCurrentKey = currentKey;
        RecordReader<KEY_IN, VALUE_IN> reader;

        // if this is the first entry, initialise all the readers appropriately
        if (keys == null || keys.isEmpty()) {
            //log.debug("Initialising the first keys");
            keys = new LinkedList<KEY_IN>();
            //keys.clear();
            for (Iterator<RecordReader<KEY_IN, VALUE_IN>> iter = readers.iterator(); iter.hasNext();) {
                reader = iter.next();

                // if the reader is before the first record, read the first
                if (reader.getCurrentKey() == null) {
                    if (!reader.nextKeyValue()) {
                        // seems we received an empty reader
                        iter.remove();
                        //log.debug("Reader initialised with no data, so removed");
                    } else {
                        //log.debug("Reader initialised with first key: " + reader.getCurrentKey());
                        keys.add(reader.getCurrentKey());
                        // store the current key we are working on
                        if (currentKey == null || reader.getCurrentKey().compareTo(currentKey) < 0) {
                            currentKey = reader.getCurrentKey();
                        }
                    }
                }

            }
        } else { // not the first entry, so we find the lowest key in the keys
            currentKey = null;
            for (KEY_IN k : keys) {
                if (currentKey == null || k.compareTo(currentKey) <= 0) {
                    currentKey = k;
                }
            }
        }

        //log.debug("Working on key[" + currentKey + "], keys[" + keys + "], readers[" + readers.size() + "]");

        // so we now know the key we are working on, we need to read everything any reader can provide
        // with that key, and once read, move the cursor onwards.  It is important to remember that if
        // we are working on key:4, and we read all the values, the key will end up being the next key
        // and not the current key, ready for the next read
        //currentValues = new LinkedList<VALUE_IN>();
        currentValues = new LinkedList<VALUE_IN>();
        //currentValues.clear();
        int index = 0;
        for (Iterator<RecordReader<KEY_IN, VALUE_IN>> iter = readers.iterator(); iter.hasNext();) {
            reader = iter.next();

            // read all content for readers on the current key

            while (reader.getCurrentKey().compareTo(currentKey) == 0) {

                if (arch == 1) {
                    int partition = partitioner.getPartition(currentKey, null, numReducers);

                    if (partition == reducerID) {
                        currentValues.add(reader.getCurrentValue());
                    }
                } else if (arch == 2 || arch == 3 || arch == 4) {
                    currentValues.add(reader.getCurrentValue());
                }

                // move the reader on, removing if it is finished
                if (!reader.nextKeyValue()) {
                    //log.debug("Removing reader, since all content read");
                    iter.remove();
                    keys.remove(index);
                    // we remove one as the index increments each reader, so will increment below
                    index--;
                    break;
                } else {
                    keys.set(index, reader.getCurrentKey());
                }
            }

            // note the comment above
            index++;

        }

        //log.debug(keys.size() + " keys are active, the lowest being [" + currentKey + "], previous[" + oldCurrentKey + "], keys[" + keys + "]");
        //System.out.println(keys.size() + " keys are active, the lowest being [" + currentKey + "], previous["+ oldCurrentKey +"], keys[" + keys + "]");

        if (currentKey == null // no data, regardless of what came before
                || (oldCurrentKey != null && oldCurrentKey.equals(currentKey))) { // same key
            return false;
        }
        return true;
    }

    public class KeyComparator implements Comparator<KEY_IN> {

        @Override
        public int compare(KEY_IN o1, KEY_IN o2) {
            return o1.compareTo(o2);
        }
    }

    /**
     * Increments the readers, assembles the next collection of values
     * [Potentially we could sort the values here, but it is not expected that
     * most implementations require or expect sorted values so this
     * implementation does not sort the values at this time]
     *
     * @return true if there are more to read, false if we are all done
     */
    public final boolean nextKeyValues1(Partitioner partitioner, int numReducers, int reducerID) {
        KEY_IN oldCurrentKey = currentKey;
        RecordReader<KEY_IN, VALUE_IN> reader;

        // if this is the first entry, initialise all the readers appropriately
        if (keys == null || keys.isEmpty()) {
            //log.debug("Initialising the first keys");
            if (!readers.isEmpty()) {
                keys = new LinkedList<KEY_IN>();
                pqKeys = new PriorityQueue<KEY_IN>(readers.size(), comparator);
            }
            //keys.clear();
            for (Iterator<RecordReader<KEY_IN, VALUE_IN>> iter = readers.iterator(); iter.hasNext();) {
                reader = iter.next();

                // if the reader is before the first record, read the first
                if (reader.getCurrentKey() == null) {
                    if (!reader.nextKeyValue()) {
                        // seems we received an empty reader
                        iter.remove();
                        //log.debug("Reader initialised with no data, so removed");
                    } else {
                        //log.debug("Reader initialised with first key: " + reader.getCurrentKey());
                        keys.add(reader.getCurrentKey());
                        pqKeys.add(reader.getCurrentKey());
                        currentKey = pqKeys.peek();
                        // store the current key we are working on
//                        if (currentKey == null || reader.getCurrentKey().compareTo(currentKey) < 0) {
//                            currentKey = reader.getCurrentKey();
//                        }
                    }
                }

            }
        } else { // not the first entry, so we find the lowest key in the keys
            currentKey = null;
            currentKey = pqKeys.peek();
//            for (KEY_IN k : keys) {
//                if (currentKey == null || k.compareTo(currentKey) <= 0) {
//                    currentKey = k;
//                }
//            }
        }

        //log.debug("Working on key[" + currentKey + "], keys[" + keys + "], readers[" + readers.size() + "]");

        // so we now know the key we are working on, we need to read everything any reader can provide
        // with that key, and once read, move the cursor onwards.  It is important to remember that if
        // we are working on key:4, and we read all the values, the key will end up being the next key
        // and not the current key, ready for the next read
        //currentValues = new LinkedList<VALUE_IN>();
        currentValues = new LinkedList<VALUE_IN>();
        //currentValues.clear();
        int index = 0;
        for (Iterator<RecordReader<KEY_IN, VALUE_IN>> iter = readers.iterator(); iter.hasNext();) {
            reader = iter.next();

            // read all content for readers on the current key

            while (reader.getCurrentKey().compareTo(currentKey) == 0) {

                if (arch == 1) {
                    int partition = partitioner.getPartition(currentKey, null, numReducers);

                    if (partition == reducerID) {
                        currentValues.add(reader.getCurrentValue());
                    }
                } else if (arch == 2 || arch == 3 || arch == 4) {
                    currentValues.add(reader.getCurrentValue());
                }

                // move the reader on, removing if it is finished
                if (!reader.nextKeyValue()) {
                    //log.debug("Removing reader, since all content read");
                    iter.remove();
                    pqKeys.remove(keys.remove(index));
                    // we remove one as the index increments each reader, so will increment below
                    index--;
                    break;
                } else {
                    pqKeys.remove(keys.get(index));
                    keys.set(index, reader.getCurrentKey());
                    pqKeys.add(reader.getCurrentKey());
                }
            }

            // note the comment above
            index++;

        }

        //log.debug(keys.size() + " keys are active, the lowest being [" + currentKey + "], previous[" + oldCurrentKey + "], keys[" + keys + "]");
        //System.out.println(keys.size() + " keys are active, the lowest being [" + currentKey + "], previous["+ oldCurrentKey +"], keys[" + keys + "]");

        if (currentKey == null // no data, regardless of what came before
                || (oldCurrentKey != null && oldCurrentKey.equals(currentKey))) { // same key
            return false;
        }
        return true;
    }

    /**
     * @return The key from the current record
     */
    public final KEY_IN getCurrentKey() {
        return currentKey;
    }

    /**
     * Return type might change in the future... this was a first guess
     * implementation
     *
     * @return The iterable values from the current readers
     */
    public final Iterator<VALUE_IN> getCurrentValues() {
        return currentValues.iterator();
    }
}
