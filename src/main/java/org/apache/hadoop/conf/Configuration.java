/**
 *
 */
package org.apache.hadoop.conf;

import java.util.HashMap;
import java.util.Map;

/**
 * This class represents the Job configuration
 *
 * This has LIMITED functionality compared to the Hadoop version, and likely to
 * be extended in the future based on demand.
 *
 * @author tim
 */
public class Configuration {
    // maintain all config in a Map

    private Map<String, Object> config = new HashMap<String, Object>();
    //protected Map<String, int> confInt = new HashMap<int, Object>();

    /**
     * @return the config for the key or null if not found
     */
    public String get(final String key) {
        if (config.containsKey(key)) {
            return config.get(key).toString();
        }
        return null;
    }

    public final void set(final String key, final String value) {
        config.put(key, value);
    }

    public final void setInt(final String key, final int value) {
        config.put(key, value);
    }

    public final int getInt(final String key, final int value) {
        return ((Integer) config.get(key)).intValue();
    }

    public final int getInt(final String key) {
        return ((Integer) config.get(key)).intValue();
    }
    
    public final void setFloat(final String key, final float value) {
        config.put(key, value);
    }

    public final float getFloat(final String key, final float value) {
        return ((Float) config.get(key)).floatValue();
    }

    public final void setBoolean(final String key, final boolean value) {
        config.put(key, value);
    }

    public final boolean getBoolean(final String key, final boolean value) {
        return ((Boolean) config.get(key)).booleanValue();
    }
}
