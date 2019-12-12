package eps.scp;

import com.google.common.collect.HashMultimap;
import org.apache.commons.lang3.ObjectUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.NumberFormat;
import java.util.Random;

public class MyThreadB implements Runnable{
    private int number;
    private String outputDirectory;
    public Thread thread;
    private File file;
    private long initialChar, finalChar;
    private String n;
    private HashMultimap<String, Long> Hash;
    private int KeySize;

    MyThreadB(int number, int KeySize, File file, long initialChar, long finalChar, HashMultimap<String, Long> hash) {
        this.thread = new Thread(this);
        this.number = number;
        this.file = file;
        this.initialChar = initialChar;
        this.finalChar = finalChar;
        this.Hash = hash;
        this.KeySize = KeySize;
        this.n = "T" + number;
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


    }
    // Método que añade una k-word y su desplazamiento en el HashMap.
    private void AddKey(String key, long offset){
        Hash.put(key, offset);
        //System.out.print(offset+"\t-> "+key+"\r");
    }
}
