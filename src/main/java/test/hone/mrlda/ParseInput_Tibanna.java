/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package test.hone.mrlda;

import edu.umd.cloud9.util.map.HMapII;
import java.io.*;
import java.util.HashMap;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ReadFileThread;
import org.apache.hadoop.io.Text;

/**
 *
 * @author ashwinkayyoor
 */
public class ParseInput_Tibanna {

    public static void parseCorpus(String inputDir, String outputDir) throws FileNotFoundException, IOException {
        String inputDocument = "Mr. LDA is a Latent Dirichlet Allocation topic modeling package based on Variational Bayesian learning approach using MapReduce and Hadoop";

        String strLine;
        int documentId = 0;
        int part = 1;
        int termID;
        int id = 0;

        IntWritable docId = new IntWritable(-1);
        HashMap<String, Integer> termIDMap = new HashMap<String, Integer>();
        String term = "";

        String inputDirectoryPath = inputDir;
        File dir = new File(inputDirectoryPath);
        for (File child : dir.listFiles()) {
            if (".".equals(child.getName()) || "..".equals(child.getName())) {
                continue;  // Ignore the self and parent aliases.
            }
            if (child.getName().toCharArray()[0] != '.' && child.isFile() && (!child.isDirectory())) {
                FileInputStream fstream = new FileInputStream(child);
                DataInputStream in = new DataInputStream(fstream);
                BufferedReader br = new BufferedReader(new InputStreamReader(in));

                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(baos));

                while ((inputDocument = br.readLine()) != null) {
                    //documentId = Integer.parseInt(inputDocument.split("\\t")[0]);

                    documentId++;
                    docId.set(documentId);
                    inputDocument = inputDocument.split("\\t")[1];
                    Document outputDocument = new Document();
                    HMapII content = new HMapII();
                    StringTokenizer stk = new StringTokenizer(inputDocument);

                    while (stk.hasMoreTokens()) {
                        term = stk.nextToken();
                        if (termIDMap.containsKey(term)) {
                            termID = termIDMap.get(term);
                        } else {
                            id++;
                            termID = id;
                            termIDMap.put(term, termID);
                        }
                        content.increment(termID, 1);
                    }
                    outputDocument.setDocument(content);
                    docId.write(dos);
                    outputDocument.write(dos);
                }
                
                dos.flush();
                dos.close();
                baos.flush();
                br.close();
                
                FileOutputStream fos = new FileOutputStream(new File(outputDir + "part" + part++));
                fos.write(baos.toByteArray());
                fos.close();
            }
        }
    }

    public static void main(String[] args) throws FileNotFoundException, IOException {
        if (args.length != 2) {
            System.out.println("Total given args: " + args.length);
            System.out.println("args: InputDir OutputDir");
            System.exit(-1);
        }
        parseCorpus(args[0], args[1]);
    }
}