/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.util;

/**
 *
 * @author ashwinkayyoor
 */
public class MathUtil {

    public static double pow(final double a, final double b) {
        final long tmp = Double.doubleToLongBits(a);
        final long tmp2 = (long) (b * (tmp - 4606921280493453312L)) + 4606921280493453312L;
        return Double.longBitsToDouble(tmp2);
    }

    public static double pow1(final double a, final double b) {
        final int x = (int) (Double.doubleToLongBits(a) >> 32);
        final int y = (int) (b * (x - 1072632447) + 1072632447);
        return Double.longBitsToDouble(((long) y) << 32);
    }
}
