/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.mapred;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author ashwinkayyoor
 */
public class Reporter {

    private Map<Enum, Integer> counterMap;
    private int currentValue;

    public Reporter() {
        counterMap = new HashMap<Enum, Integer>();
    }

    public void incrCounter(Enum enumKey, int value) {
        if (counterMap.containsKey(enumKey)) {
            currentValue = counterMap.get(enumKey) + value;
            counterMap.put(enumKey, currentValue);
        } else {
            counterMap.put(enumKey, value);
        }
    }
}
