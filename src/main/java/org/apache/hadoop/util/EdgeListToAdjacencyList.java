/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.util;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;

/**
 *
 * @author ashwinkayyoor
 */
public class EdgeListToAdjacencyList {

    public void edgeListToAdjacencyList(String inputFileName, String outputFileName) throws FileNotFoundException, IOException {
        FileReader freader = new FileReader(inputFileName);
        BufferedReader breader = new BufferedReader(freader);
        HashMap<String, HashSet<String>> adjList = new HashMap();

        String[] node;
        String line = "";
        HashSet<String> list;
        while ((line = breader.readLine()) != null) {
            node = line.split(" ");
            if (adjList.containsKey(node[0])) {
                list = adjList.get(node[0]);
                list.add(node[1]);
                adjList.put(node[0], list);
            } else {
                list = new HashSet<String>();
                list.add(node[1]);
                adjList.put(node[0], list);
            }
        }
        freader.close();
        breader.close();

        Object[] keys = adjList.keySet().toArray();
        int len = keys.length;

        FileWriter fwriter = new FileWriter(outputFileName);
        BufferedWriter bwriter = new BufferedWriter(fwriter);
        HashSet<String> values;
        line = "";
        for (int i = 0; i < len; ++i) {
            line += keys[i] + " ";
            values = adjList.get((String)keys[i]);
            for (String adjNode : values) {
                line += adjNode + " ";
            }
            line += "\n";
            bwriter.write(line);
            line = "";
        }
        bwriter.flush();
        bwriter.close();
    }

    public static void main(String[] args) {
        
        //edgeListToAdjacencyList("");
        
        if (args.length != 2) {
            System.err.println("Argument: inputEdgeFileName outputAdjcencyListFileName");
            System.exit(-1);
        }
    }
}
