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
    /*private int numeroDifKeysGlobal=0;
    private int numeroTotKeysGlobal=0;
    private long numeroOffsetsGlobal=0;
    private int numeroBytes=0;
    private int currentProgress=0;*/
    private int offsetGlobal = 0;

    static ReentrantLock bl = new ReentrantLock();
    static Semaphore llegada = new Semaphore(1);    //permiso a 1
    static Semaphore salida = new Semaphore(0);     //permiso a 0
    static int barrierCounter = 0;

    MyThreadB(int number, int KeySize, File file, long initialChar, long finalChar, HashMultimap<String, Long> hash,
              int nThreads,int Progress,int totalChars) {
        this.thread = new Thread(this);
        this.number = number;
        this.file = file;
        this.initialChar = initialChar;
        this.finalChar = finalChar;
        //this.Hash = hash;
        this.KeySize = KeySize;
        this.n = "T" + number;
        this.nThreads = nThreads;
        this.progress = Progress;
        this.totalChars = totalChars;
        System.out.println("Thread n" + number + " creado");
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
                //bl.lock();
                //numeroOffsetsGlobal++;
                //bl.unlock();
                if (car=='\n' || car=='\r' || car=='\t') {
                    // Sustituimos los carácteres de \n,\r,\t en la clave por un espacio en blanco.
                    if (key.length()==KeySize && key.charAt(KeySize-1)!=' ')
                        key = key.substring(1, KeySize) + ' ';
                        synchronized (this){
                            InvertedIndexConc.diffKeysTotalesGeneradas.incrementAndGet();
                        }
                    offsetGlobal = updateOffset();
                    continue;
                }
                if (key.length()<KeySize) {
                    // Si la clave es menor de K, entonces le concatenamos el nuevo carácter leído.
                    key = key + (char) car;
                    synchronized (this){
                        InvertedIndexConc.diffKeysTotalesGeneradas.incrementAndGet();
                    }
                }else {
                    // Si la clave es igua a K, entonces eliminaos su primier carácter y le concatenamos el nuevo
                    // carácter leído (implementamos una slidding window sobre el fichero a indexar).
                    key = key.substring(1, KeySize) + (char) car;
                    synchronized (this){
                        InvertedIndexConc.diffKeysTotalesGeneradas.incrementAndGet();
                    }
                }
                if (key.length()==KeySize) {

                    if (InvertedIndexConc.Hash.get(key).isEmpty()) {  //Si la key no se encuentra en la hash
                        updateDifKeysProcesadas();  //incrementamos la variable de distintas keys procesadas
                    }
                    // Si tenemos una clave completa, la añadimos al Hash, junto a su desplazamiento dentro del fichero.
                    AddKey(key, offset - KeySize + 1);
                    bl.lock();
                    InvertedIndexConc.numBytesTotalesEscritos.addAndGet(KeySize);   //Añadimos el size de la key para calcular el valor total de bytes escritos
                    bl.unlock();
                }
                //ESTA MALAMENT
                synchronized (this){
                    InvertedIndexConc.numBytesTotalesLeidos.getAndSet((int) offset);
                }
                offsetGlobal = updateOffset();
                if(offsetGlobal%(int)(totalChars * (progress / 100.0))==0){
                    bl.lock();
                    updateProgress();
                    bl.unlock();
                    synchronized (this) {
                        while (!InvertedIndexConc.showProgress()) {
                            try {
                                this.wait();
                            } catch (java.lang.InterruptedException e) {
                            }
                        }
                        this.notify();
                    }
                }
            }


            //System.out.println("LENGTH:" + raf.length());
            raf.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        act_as_a_barrier();
        sincro = true;
    }
    private void updateProgress(){              //Actualiza el progreso total

        InvertedIndexConc.currentProgress.addAndGet(progress);


    }
    private void updateDifKeysProcesadas() {      //Actualiza las distintas keys procesadas
        synchronized (this){
            InvertedIndexConc.diffKeysTotalesProcesadas.incrementAndGet();
        }
    }

    private int updateOffset() {        //Actualiza y devuelve el Offset actual
        synchronized (this){
            return InvertedIndexConc.actualOffset.incrementAndGet();
        }
    }

    public boolean getSincro(){     //para saber si se han sincronizado todos los hilos des del InvertedIndexConc
        return sincro;
    }

    // Método que añade una k-word y su desplazamiento en el HashMap.
    private synchronized void AddKey(String key, long offset){
        InvertedIndexConc.Hash.put(key, offset);
        //System.out.print(offset+"\t-> "+key+"\r");
    }

    void act_as_a_barrier() {
        try {
            llegada.acquire();              //Adquiere el permiso de llegada (1)
        } catch (InterruptedException e1) {
        }
        bl.lock();                          //sincronizamos hilos
        barrierCounter++;                   //Incrementamos la variable global para controlar el numero de hilos que
                                            //han terminado
        bl.unlock();
        if (barrierCounter < nThreads) {    //Si el contador global es mas pequeño que el numero de threads totales
            llegada.release();              //liberamos el permiso de entrada, falta por llegar más threads
        } else {
            salida.release();               //liberamos el permiso de salida porque ya han llegado todos
        }
        try {
            salida.acquire();               //adquirmos el permiso de salida
        } catch (InterruptedException e) {
        }
        bl.lock();
        barrierCounter--;                   //cada hilo decrementa la variable global asociada al numero de hilos
        bl.unlock();
        if (barrierCounter > 0) {           //En caso que el contador aun sea mayor de 0
            salida.release();               //liberaremos el permiso de salida porque ya han llegado todos
        } else {
            llegada.release();              //liberaremos el permiso de entrada porque
        }
    }
}
