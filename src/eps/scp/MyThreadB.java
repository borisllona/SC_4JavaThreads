package eps.scp;

import com.google.common.collect.HashMultimap;
import org.apache.commons.lang3.ObjectUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.NumberFormat;
import java.util.Map;
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
    private int progress;  //Marca cada cuantos caracteres hay que printar el progreso.
    private int totalChars;

    //Estadisticas
    private int numeroKeys=0;
    private long numeroOffsets=0;
    private int numeroBytes=0;
    private int currentProgress=0;

    static ReentrantLock bl = new ReentrantLock();
    static Semaphore llegada = new Semaphore(1);    //permiso a 1
    static Semaphore salida = new Semaphore(0);     //permiso a 0
    static volatile int barrierCounter = 0;

    MyThreadB(int number, int KeySize, File file, long initialChar, long finalChar, HashMultimap<String, Long> hash, int nThreads,int Progress,int totalChars) {
        this.thread = new Thread(this);
        this.number = number;
        this.file = file;
        this.initialChar = initialChar;
        this.finalChar = finalChar;
        this.Hash = hash;
        this.KeySize = KeySize;
        this.n = "T" + number;
        this.nThreads = nThreads;
        this.progress = Progress;
        this.totalChars = totalChars;
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
                    /*if(Hash.get(key) == null){
                        System.out.println("KEY NO EN HASH");
                        bl.lock();
                        numeroKeys++;
                        bl.unlock();
                    }*/
                    // Si tenemos una clave completa, la añadimos al Hash, junto a su desplazamiento dentro del fichero.
                    AddKey(key, offset-KeySize+1);

                int up = updateProgess();
                /*System.out.println("Updated progress is:" + up);
                System.out.println((totalChars * (progress / 100.0)));*/
                if(up%(int)(totalChars * (progress / 100.0))==0){
                    actualizarEstadisticasGlobales(offset);
                    System.out.println("--------------------------------");
                    System.out.println("Updated progress is:" + up);
                    System.out.println((totalChars * (progress / 100.0)));
                    System.out.println("SHA DE MOSTRAR");
                    showProgress();
                }
            }
            raf.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        act_as_a_barrier();
        sincro = true;
    }

    private void actualizarEstadisticasGlobales(long offset) {
        bl.lock();
        numeroOffsets=numeroOffsets + offset;   //Offsets gobales hasta el momento
        bl.unlock();

    }

    public void showProgress() {
        System.out.println("++++++++++ESTADISTICAS GLOBALES+++++++++");
        System.out.println("++++++++++++++++++++++++++++++++++++++++");
        System.out.println("Numero de keys diferentes totales:" + numeroKeys);
        System.out.println("Numero values (offset) totales:" + numeroOffsets);
        System.out.println("Numero de bytes totales leidos del fichero:");
        System.out.println("Progreso total de la construcción del índice:");
        System.out.println("++++++++++++++++++++++++++++++++++++++++");

    }

    private int updateProgess() {
        synchronized (this){
            return InvertedIndexConc.actualProgress.incrementAndGet();
        }
    }
    public boolean getSincro(){
        return sincro;
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
