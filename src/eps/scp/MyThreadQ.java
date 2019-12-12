package eps.scp;

import com.google.common.collect.HashMultimap;

import java.io.*;

public class MyThreadQ implements Runnable {
    //public final String DIndexFilePrefix = "/IndexFile";

    private int number;
    //private String outputDirectory;
    public Thread thread;
    private File[] listOfFiles;
    private long initialFile, finalFile;
    private String n;
    public HashMultimap<String, Long> Hash;

    MyThreadQ(int number, File[] listOfFiles, long initialFile, long finalFile, HashMultimap<String, Long> hash) {
        this.thread = new Thread(this);
        this.number = number;
        this.listOfFiles = listOfFiles;
        this.initialFile = initialFile;
        this.finalFile = finalFile;
        this.Hash = HashMultimap.create();
        this.n = "T" + number;
        System.out.println("Thread n" + number + " creado");
    }
    public HashMultimap<String, Long> getHash(){
        return this.Hash;
    }
    @Override
    public void run() {
        // Recorremos todos los ficheros del directorio de Indice y los procesamos.
        long numF = 0;
        //System.out.println(initialFile + " - " + finalFile);
        for (numF = initialFile; numF < finalFile; numF++) {
            File file = listOfFiles[(int)numF];
            if (file.isFile()) {
                //System.out.println("Processing file " + folder.getPath() + "/" + file.getName()+" -> ");
                try {
                    FileReader input = new FileReader(file);
                    BufferedReader bufRead = new BufferedReader(input);
                    String keyLine = null;
                    try {
                        // Leemos fichero línea a linea (clave a clave)
                        while ((keyLine = bufRead.readLine()) != null) {
                            // Descomponemos la línea leída en su clave (k-word) y offsets
                            String[] fields = keyLine.split("\t");
                            String key = fields[0];
                            String[] offsets = fields[1].split(",");
                            // Recorremos los offsets para esta clave y los añadimos al HashMap
                            for (int i = 0; i < offsets.length; i++) {
                                long offset = Long.parseLong(offsets[i]);
                                Hash.put(key, offset);
                            }
                        }
                    } catch (IOException e) {
                        System.err.println("Error reading Index file");
                        e.printStackTrace();
                    }
                } catch (FileNotFoundException e) {
                    System.err.println("Error opening Index file");
                    e.printStackTrace();
                }
                //System.out.println("");
            }
        }
    }
}
