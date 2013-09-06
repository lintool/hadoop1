/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.util;

/**
 *
 * @author ashwinkayyoor
 */
import java.util.Random;

public class ZipfGenerator {

    private Random rnd = new Random(System.currentTimeMillis());
    private int size;
    private double skew;
    private double bottom = 0;
    private double[] power;
    private double[] sum_prob;

    public ZipfGenerator(int size, double skew) {
        this.size = size+1;
        this.skew = skew;

        power = new double[this.size + 1];
        sum_prob = new double[this.size + 1];

        for (int i = 1; i < this.size; i++) {
            this.bottom += (1 / MathUtil.pow(i, this.skew));
        }

        double sumprob = 0;
        double c = 1 / this.bottom;
        for (int i = 0; i <= this.size; ++i) {
            //for (int j = 0; j <= 100; ++j) {
            power[i] = MathUtil.pow(i, skew);
            if (i != 0) {
                sumprob = sumprob + c / MathUtil.pow((double) i, this.skew);
            } else {
                sumprob = 0;
            }
            sum_prob[i] = sumprob;
            //}
        }
    }

    //===========================================================================
//=  Function to generate Zipf (power law) distributed random variables     =
//=    - Input: alpha and N                                                 =
//=    - Output: Returns with Zipf distributed random variable              =
//===========================================================================
    public int next() {

        //double c = 0;          // Normalization constant
        double z;                     // Uniform random number (0 < z < 1)
        //double sum_prob;              // Sum of probabilities
        double zipf_value = 0;            // Computed exponential value to be returned
        int i;                     // Loop counter
        double alpha = this.skew;
        int n = this.size;
        // Compute normalization constant on first call only
        //c = 1 / this.bottom;
        // Pull a uniform random number (0 < z < 1)

        do {
            z = rnd.nextDouble();
        } while ((z == 0) || (z == 1));

        //      z = rnd.nextDouble();
        // Map z to the value
        //double sum_prob = 0;
        for (i = 0; i <= n; i++) {
            //sum_prob = sum_prob + c / MathUtil.pow((double) i, alpha);
            if (sum_prob[i] >= z) {
                zipf_value = i;
                break;
            }
        }

        // Assert that zipf_value is between 1 and N
        assert ((zipf_value >= 1) && (zipf_value <= n));

        return (int) zipf_value-1;
    }
    // the next() method returns an random rank id.
    // The frequency of returned rank ids are follows Zipf distribution.

    public int zipf() {
        int rank = 0;
        double friquency = 0;
        double dice = 0;

//        rank = rnd.nextInt(size);
//        friquency = (1.0d / MathUtil.pow(rank + 1, this.skew)) / this.bottom;
//        dice = rnd.nextDouble();

        int cnt = 0;
        while (!(dice < friquency)) {
            rank = rnd.nextInt(size);
            //friquency = (1.0d / MathUtil.pow(rank + 1, this.skew)) / this.bottom;
            friquency = 1.0d / (power[rank + 1] * this.bottom);

            dice = rnd.nextDouble();
            cnt++;
        }

        //System.out.println("count: " + cnt);
        //rank = rnd.nextInt(size);

        return rank;
    }

    // This method returns a probability that the given rank occurs.
    public double getProbability(int rank) {
        return (1.0d / MathUtil.pow(rank, this.skew)) / this.bottom;
    }

    public static void main(String[] args) {
//   if(args.length != 2) {
//     System.out.println("usage: ./zipf size skew");
//     System.exit(-1);
//   }
        String size = "128";
        String skew = "0.8";
        ZipfGenerator zipfObj = new ZipfGenerator(Integer.valueOf(size),
                Double.valueOf(skew));
        int[] count = new int[Integer.valueOf(size)+1];
        for (int i = 1; i <= 10000; i++) {
            //System.out.println(i + " " + zipf.getProbability(i));
            //System.out.println(i + " " + (int) zipfObj.zipf());
            count[(int) zipfObj.next()]++;
        }

        for (int i = 0; i < Integer.valueOf(size); ++i) {
            System.out.println(i + " " + count[i]);
        }
    }
}