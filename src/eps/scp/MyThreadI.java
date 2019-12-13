package eps.scp;

import com.google.common.collect.HashMultimap;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MyThreadI implements Runnable{
    public final String DIndexFilePrefix = "/IndexFile";

    private int number;
    private String outputDirectory;
    public Thread thread;
    private ArrayList<String> list;
    private long files;
    private String n;
    private HashMultimap<String, Long> Hash;
    private int nThreads;

    static Lock bl = new ReentrantLock();
    static Semaphore llegada = new Semaphore(1);    //permiso a 1
    static Semaphore salida = new Semaphore(0);     //permiso a 0
    static volatile int barrierCounter = 0;

    MyThreadI(int number, ArrayList<String> list, String outputDirectory, int files, HashMultimap<String, Long> hash, int nThreads){
        this.thread = new Thread(this);
        this.number = number;
        this.list = list;
        this.files = files;
        this.outputDirectory = outputDirectory;
        this.n = "T"+number;
        this.Hash = hash;
        this.nThreads = nThreads;
        System.out.println("Thread n"+number+" creado");
    }

    @Override
    public void run() {
        long remainingKeys = this.list.size(), remainingFiles = this.files, keysByFile = 0;
        String key = "";
        Iterator keyIterator = list.iterator();
        for (int f=1;f<=this.files;f++)
        {
            try {
                //Asignamos un nombre distinto a cada archivo, formado por su ruta, el nombre, numero de thread y numero de fichero dentro del thread.
                File KeyFile = new File(outputDirectory + DIndexFilePrefix +n+ String.format("%03d", f));
                FileWriter fw = new FileWriter(KeyFile);
                BufferedWriter bw = new BufferedWriter(fw);
                // Calculamos el número de claves a guardar en este fichero.
                keysByFile =  remainingKeys / remainingFiles;
                remainingKeys -= keysByFile;
                // Recorremos las claves correspondientes a este fichero.
                while (keyIterator.hasNext() && keysByFile>0) {
                    key = (String) keyIterator.next();
                    SaveIndexKey(key,bw);  // Salvamos la clave al fichero.
                    keysByFile--;
                }
                bw.close(); // Cerramos el fichero.
                remainingFiles--;
            } catch (IOException e) {
                System.err.println("Error opening Index file " + outputDirectory + "/IndexFile" + f);
                e.printStackTrace();
                System.exit(-1);
            }
        }
        act_as_a_barrier();

    }


    // Método para salvar una clave y sus ubicaciones en un fichero.
    public void SaveIndexKey(String key, BufferedWriter bw)
    {
        try {
            Collection<Long> values = Hash.get(key);
            ArrayList<Long> offList = new ArrayList<Long>(values);
            // Creamos un string con todos los offsets separados por una coma.
            String joined = StringUtils.join(offList, ",");
            bw.write(key+"\t");
            bw.write(joined+"\n");
        } catch (IOException e) {
            System.err.println("Error writing Index file");
            e.printStackTrace();
            System.exit(-1);
        }
    }

    // Método para cargar en memoria (HashMap) el índice invertido desde su copia en disco.
    private void act_as_a_barrier() {
        try {
            llegada.acquire();
        } catch (InterruptedException e1) {}
        bl.lock();
        barrierCounter++;
        System.out.println(barrierCounter);
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
