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
public class ParseInput {

    public static void parseCorpus() throws FileNotFoundException, IOException {
        String inputDocument = "Mr. LDA is a Latent Dirichlet Allocation topic modeling package based on Variational Bayesian learning approach using MapReduce and Hadoop";

        String strLine;
        int documentId = 0;
        int part = 1;
        //int termID;
        int id = 0;

        IntWritable docId = new IntWritable(-1);
        HashMap<String, Integer> termIDMap = new HashMap<String, Integer>();
        String term = "";

        String inputDir="data/mrlda/input/data";
        String outputDir="data/mrlda/input/iter0000/";
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
                DataOutputStream dos = new DataOutputStream(baos);

                while ((inputDocument = br.readLine()) != null) {
                    //documentId = Integer.parseInt(inputDocument.split("\\t")[0]);

                    documentId++;
                    docId.set(documentId);
                    inputDocument = inputDocument.split("\\t")[1];
                    Document outputDocument = new Document();
                    HMapII content = new HMapII();
                    StringTokenizer stk = new StringTokenizer(inputDocument);

                    int termID;
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
                baos.close();                
                fos.close();
            }
        }
    }

    public static void main(String[] args) throws FileNotFoundException, IOException {        
        parseCorpus();
    }
}