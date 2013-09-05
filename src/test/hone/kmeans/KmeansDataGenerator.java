/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package test.hone.kmeans;

import java.io.*;
import java.util.Random;

/**
 *
 * @author ashwinkayyoor
 */
public class KmeansDataGenerator {

    public static void generateData(long size, int maxNumber) throws FileNotFoundException, IOException {
        long timeInMillis = System.currentTimeMillis();
        Random generator = new Random(19580427 + timeInMillis);

        FileWriter fw = new FileWriter("data/kmeans/kmeansData.txt");
        PrintWriter pw = new PrintWriter(fw);

        String str = "";
        for (long i = 0L; i < size;) //Close the output stream
        {
            str = generator.nextInt(maxNumber) + "," + generator.nextInt(maxNumber);
            //dos.writeUTF(str);
            pw.println(str);
            i += str.length() + 2;
        }

        pw.flush();
        pw.close();
    }

    public static void generateCenters(int number, int maxNumber) throws FileNotFoundException, IOException {
        long timeInMillis = System.currentTimeMillis();
        Random generator = new Random(19580427 + timeInMillis);

        FileWriter fw = new FileWriter("data/kmeans/kmeansCenters" + number + ".txt");
        PrintWriter pw = new PrintWriter(fw);

        String str = "";
        for (int i = 0; i < number; ++i) //Close the output stream
        {
            str = generator.nextInt(maxNumber) + "," + generator.nextInt(maxNumber);
            //dos.writeUTF(str);
            pw.println(str);
        }

        pw.flush();
        pw.close();
    }

    public static void main(String[] args) throws FileNotFoundException, IOException {
        // generateData(8589934592L, 10000);
        generateData(1024*1024, 10000);
        generateCenters(10, 10000);
    }
}
