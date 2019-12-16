package eps.scp;

import com.google.common.collect.HashMultimap;

import java.io.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MyThreadQ implements Runnable {
    //public final String DIndexFilePrefix = "/IndexFile";

    private int number;
    public boolean sincro;
    private int nThreads;
    //private String outputDirectory;
    public Thread thread;
    private File[] listOfFiles;
    private long initialFile, finalFile;
    private String n;
    public HashMultimap<String, Long> Hash;
    Lock l = new ReentrantLock();

    static ReentrantLock bl = new ReentrantLock();
    private static Semaphore llegada = new Semaphore(1);    //permiso a 1
    private static Semaphore salida = new Semaphore(0);     //permiso a 0
    private static int barrierCounter = 0;

    MyThreadQ(int number, File[] listOfFiles, long initialFile, long finalFile, HashMultimap<String, Long> hash,int nThreads) {
        this.thread = new Thread(this);
        this.number = number;
        this.listOfFiles = listOfFiles;
        this.initialFile = initialFile;
        this.finalFile = finalFile;
        //this.Hash = hash;
        this.n = "T" + number;
        this.nThreads = nThreads;
        System.out.println("Thread n" + number + " creado");
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
                                l.lock();
                                InvertedIndexConc.Hash.put(key, offset);

                                l.unlock();
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
        act_as_a_barrier();
        sincro = true;
    }
    private void act_as_a_barrier() {
        try {
            llegada.acquire();
        } catch (InterruptedException e1) {}
        bl.lock();
        barrierCounter++;
        bl.unlock();
        if (barrierCounter < nThreads) {
            llegada.release();
        } else {
            salida.release();
        }
        try {
            salida.acquire();
        } catch (InterruptedException e) {}
        bl.lock();
        barrierCounter--;
        bl.unlock();
        if (barrierCounter > 0) {
            salida.release();
        } else {
            llegada.release();
        }
    }
}
