package eps.scp;

import com.google.common.collect.HashMultimap;
import org.apache.commons.lang3.ObjectUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.NumberFormat;
import java.util.Random;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MyThreadB implements Runnable{
    private int number;
    private String outputDirectory;
    public Thread thread;
    private File file;
    private long initialChar, finalChar;
    private String n;
    private HashMultimap<String, Long> Hash;
    private int KeySize;
    private int nThreads;
    public boolean sincro = false;

    static ReentrantLock bl = new ReentrantLock();
    static Semaphore llegada = new Semaphore(1);    //permiso a 1
    static Semaphore salida = new Semaphore(0);     //permiso a 0
    static volatile int barrierCounter = 0;

    MyThreadB(int number, int KeySize, File file, long initialChar, long finalChar, HashMultimap<String, Long> hash, int nThreads) {
        this.thread = new Thread(this);
        this.number = number;
        this.file = file;
        this.initialChar = initialChar;
        this.finalChar = finalChar;
        this.Hash = hash;
        this.KeySize = KeySize;
        this.n = "T" + number;
        this.nThreads = nThreads;
        System.out.println("Thread n" + number + " creado");
    }

    public HashMultimap<String, Long> getHash() {
        return this.Hash;
    }

    @Override
    public void run() {
        //FileInputStream is;
        long offset = -1;
        int car;
        String key="";
       // System.out.println("inici" + initialChar);
        //System.out.println("final" + finalChar);

        try {
            //File tempFile = File.createTempFile("Thread", null);
            RandomAccessFile raf = new RandomAccessFile(file, "r");
            raf.seek(initialChar);

            while( raf.getFilePointer() < finalChar && (car = raf.read())!=-1)
            {
                offset++;
                if (car=='\n' || car=='\r' || car=='\t') {
                    // Sustituimos los carácteres de \n,\r,\t en la clave por un espacio en blanco.
                    if (key.length()==KeySize && key.charAt(KeySize-1)!=' ')
                        key = key.substring(1, KeySize) + ' ';
                    continue;
                }
                if (key.length()<KeySize)
                    // Si la clave es menor de K, entonces le concatenamos el nuevo carácter leído.
                    key = key + (char) car;
                else
                    // Si la clave es igua a K, entonces eliminaos su primier carácter y le concatenamos el nuevo carácter leído (implementamos una slidding window sobre el fichero a indexar).
                    key = key.substring(1, KeySize) + (char) car;

                if (key.length()==KeySize)
                    // Si tenemos una clave completa, la añadimos al Hash, junto a su desplazamiento dentro del fichero.
                    AddKey(key, offset-KeySize+1);
            }
            raf.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        act_as_a_barrier();
        sincro = true;
        System.out.println("IM Thread "+number+" sincro is "+sincro);
    }
    // Método que añade una k-word y su desplazamiento en el HashMap.
    private synchronized void AddKey(String key, long offset){
        Hash.put(key, offset);
        //System.out.print(offset+"\t-> "+key+"\r");
    }

    void act_as_a_barrier() {
        try {
            llegada.acquire();
        } catch (InterruptedException e1) {
        }
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
        } catch (InterruptedException e) {
        }
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
