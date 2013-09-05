/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.fs;

/**
 *
 * @author ashwinkayyoor
 */
public class Path {

    private String inputPath = "";
    public static String SEPARATOR = "/";

    public Path(String inputPath) {
        this.inputPath = inputPath;
    }

    @Override
    public String toString() {
        return inputPath;
    }

    /**
     * Returns the parent of a path or null if at root.
     */
    public Path getParent() {
        String path = inputPath;
        int lastSlash = path.lastIndexOf('/');
        int start = 0;
        if ((path.length() == start) || // empty path
                (lastSlash == start && path.length() == start + 1)) { // at root
            return null;
        }

        String parent;
        int end = 0;
        parent = path.substring(0, lastSlash == end ? end + 1 : lastSlash);

        return new Path(parent);
    }
}
