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
public class KmeansDataGenerator_Tibanna {

    public static void generateData(long size, int maxNumber, String datafileOutputPath) throws FileNotFoundException, IOException {
        long timeInMillis = System.currentTimeMillis();
        Random generator = new Random(19580427 + timeInMillis);

//        FileOutputStream fstream = new FileOutputStream(datafileOutputPath + "/kmeansData.txt");
//        DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(fstream));

        FileWriter fw = new FileWriter(datafileOutputPath + "/kmeansData.txt");
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
//        fw.flush();
//        fw.close();
//        dos.flush();
//        fstream.flush();
//        dos.close();
//        fstream.close();
    }

    public static void generateCenters(int number, int maxNumber, String centersOutputPath) throws FileNotFoundException, IOException {
        long timeInMillis = System.currentTimeMillis();
        Random generator = new Random(19580427 + timeInMillis);

//        FileOutputStream fstream = new FileOutputStream(centersOutputPath+"/kmeansCenters" + number + ".txt");
//        DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(fstream));

        FileWriter fw = new FileWriter(centersOutputPath + "/kmeansCenters" + number + ".txt");
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
//        fw.flush();
//        fw.close();

//        dos.flush();
//        fstream.flush();
//        dos.close();
//        fstream.close();
    }

    public static void main(String[] args) throws FileNotFoundException, IOException {
        if (args.length != 4) {
            System.out.println("Args: inputDir dataFileSize maxRandomNumber numberOfCenters");
            System.exit(-1);
        }

        generateData(Long.parseLong(args[1]), Integer.parseInt(args[2]), args[0]);
        generateCenters(Integer.parseInt(args[3]), Integer.parseInt(args[2]), args[0]);
    }
}
