/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.test;

import java.io.*;
import java.util.Properties;
import java.util.Random;

/**
 *
 * @author ashwinkayyoor
 */
public class GenerateInput {

    public static void generateRandomIntData(int maxNumber, int size) throws IOException {
        long timeInMillis = System.currentTimeMillis();
        Random generator = new Random(19580427 + timeInMillis);

        FileOutputStream fstream = new FileOutputStream("randIntData.txt");
        DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(fstream));

        String str = "";
        for (int i = 0; i < size;) //Close the output stream
        {
            str = generator.nextInt(maxNumber) + " \n";
            dos.writeUTF(str);
            i += str.length() + 2;
        }

        dos.flush();
        fstream.flush();
        dos.close();
        fstream.close();
    }

    public static void generateRandomStringData(int length, int size) throws IOException {
        long timeInMillis = System.currentTimeMillis();
        Random generator = new Random(19580427 + timeInMillis);

        FileOutputStream fstream = new FileOutputStream("data/randIntData.txt");
        DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(fstream));

        String str = "";
        String tempStr = "";
        int ind = 0;
        for (int i = 0; i < size;) //Close the output stream
        {
            if (((ind + 1) % 3) != 0) {
                tempStr = generator.nextInt(length) + " ";
                str += tempStr;
                i += tempStr.length();
            } else {
                tempStr = generator.nextInt(length) + "\n";
                str += tempStr;
                i += tempStr.length();
                dos.writeUTF(str);
                str = "";
            }
            ind++;
        }

        dos.flush();
        fstream.flush();
        dos.close();
        fstream.close();
    }

    public static void main(String[] args) throws IOException {
        Properties prop = new Properties();
        prop.load(new FileInputStream("config.properties"));

        //generateRandomIntData(100000, 9 * 1024 * 1024);
        generateRandomStringData(100000, 9437184);
    }
}
