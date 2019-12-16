package eps.scp;

import com.google.common.collect.HashMultimap;
//import sun.security.mscapi.KeyStore;

import java.io.*;
import java.lang.invoke.SwitchPoint;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by Nando on 3/10/19.
 */
public class InvertedIndexConc{
    // Constantes
    private final int DKeySize = 10;            // Tamaño de la clave/ k-word por defecto.
    private final int DIndexMaxNumberOfFiles = 1000;   // Número máximio de ficheros para salvar el índice invertido.
    private final String DIndexFilePrefix = "/IndexFile";   // Prefijo de los ficheros de Índice Invertido.
    private final float DMinimunMatchingPercentage = 0.80f;  // Porcentaje mínimo de matching entre el texto original y la consulta (80%)
    private final int DPaddingMatchText = 20;   // Al mostrar el texto original que se corresponde con la consulta se incrementa en 20 carácteres
    private final int DThreads = 5;
    private int Progress = 5;

    //estadisticas globales
    public static AtomicInteger actualOffset = new AtomicInteger(2);
    public static AtomicInteger diffKeysTotalesProcesadas = new AtomicInteger(0);
    public static AtomicInteger diffKeysTotalesGeneradas = new AtomicInteger(0);
    public static AtomicInteger currentProgress = new AtomicInteger(0);
    public static AtomicInteger numBytesTotalesLeidos = new AtomicInteger(0);
    public static AtomicInteger numBytesTotalesEscritos = new AtomicInteger(0);
    //private final int DChunkSize = 100;

    // Members
    private String InputFilePath;       // Contiene la ruta del fichero a Indexar.
    private RandomAccessFile randomInputFile;  // Fichero random para acceder al texto original con mayor porcentaje de matching.
    private int KeySize;            // Número de carácteres de la clave (k-word)
    private int nThreads;
    public static HashMultimap<String, Long> Hash = HashMultimap.create();  // Hash Map con el Índice Invertido.


    // Constructores
    public InvertedIndexConc() {
        InputFilePath = null;
        nThreads = DThreads;
        KeySize = DKeySize;
    }

    public InvertedIndexConc(String inputFile) {
        this();
        InputFilePath = inputFile;
    }

    public InvertedIndexConc(String inputFile, int numThreads) {
        this();
        InputFilePath = inputFile;
        nThreads = numThreads;
    }

    public InvertedIndexConc(String inputFile,int numThreads ,int progress) {
        InputFilePath = inputFile;
        nThreads = numThreads;
        Progress = progress/100;
    }
    public InvertedIndexConc(String inputFile,int numThreads ,int progress,int keySize) {
        InputFilePath = inputFile;
        nThreads = numThreads;
        Progress = progress;
        KeySize = keySize;
    }
    public InvertedIndexConc(int numThreads) {
        nThreads = numThreads;
    }
    public InvertedIndexConc(int numThreads, int progress) {
        nThreads = numThreads;
        Progress = progress/100;
    }
    public InvertedIndexConc(int numThreads,int keySize, int p) {
        nThreads = numThreads;
        KeySize = keySize;
    }


    public void SetFileName(String inputFile) {
        InputFilePath = inputFile;
    }

    /* Método para construir el indice invertido, utilizando un HashMap para almacenarlo en memoria */

    public void BuildIndex()
    {
        long charxThread = 0, initialchar = 0, finalchar, dif = 0, totalChars,aux;
        ArrayList<MyThreadB> thr = new ArrayList<MyThreadB>();
        File file = new File(InputFilePath);

        long fileLen = file.length();

        while(fileLen>0){
            fileLen-=nThreads;
            charxThread++;
        }

        totalChars = charxThread*nThreads;

        for (int i = 0; i < nThreads; i++){
            finalchar = initialchar + charxThread - 1;
            if(finalchar > file.length()){
                dif = finalchar - file.length();
                finalchar -= dif;
            }
            MyThreadB t = new MyThreadB(i, KeySize, file, initialchar, finalchar,Hash, nThreads,Progress,(int)totalChars);
            initialchar += charxThread;
            thr.add(t);
            t.thread.start();
        }


        while(!thr.get(0).getSincro()){ System.out.print(""); }
        System.out.println("SALGO");
    }

    public static boolean showProgress() {
        System.out.println("++++++++++ESTADISTICAS GLOBALES+++++++++");
        System.out.println("++++++++++++++++++++++++++++++++++++++++");
        System.out.println("Numero de keys diferentes generadas/procesadas: " + diffKeysTotalesGeneradas + " / " + diffKeysTotalesProcesadas);
        System.out.println("Numero values (offset) generados/procesados: " + actualOffset + " / " + actualOffset);
        System.out.println("Numero de bytes totales leidos/escritos/procesados del fichero: " + numBytesTotalesLeidos + " / "
                + numBytesTotalesEscritos + " / " );
        System.out.println("Progreso total de la construcción del índice:" + currentProgress + "%");
        System.out.println("++++++++++++++++++++++++++++++++++++++++");
        return true;
    }

    // Método para imprimir por pantalla el índice invertido.
    public void PrintIndex() {
        Set<String> keySet = Hash.keySet();
        Iterator keyIterator = keySet.iterator();
        while (keyIterator.hasNext() ) {
            String key = (String) keyIterator.next();
            System.out.print(key + "\t");
            Collection<Long> values = Hash.get(key);
            for(Long value : values){
                System.out.print(value+",");
            }
            System.out.println();
        }
    }

    // Método para salvar en disco el índice invertido.
    // Recibe la ruta del directorio en donde se van a guardar los ficheros del indice.
    public void SaveIndex(String outputDirectory) throws InterruptedException {
        int numberOfFiles, remainingFiles;
        String key = "";
        long remainingKeys = 0, keysxThread = 0, filesxThread = 0;
        Charset utf8 = StandardCharsets.UTF_8;
        Set<String> keySet = Hash.keySet();
        Set<String> keyTSet;
        ArrayList<MyThreadI> thr = new ArrayList<MyThreadI>();

        // Calculamos el número de ficheros a crear en función del núemro de claves que hay en el hash.
        if (keySet.size() > DIndexMaxNumberOfFiles)
            numberOfFiles = DIndexMaxNumberOfFiles;
        else
            numberOfFiles = keySet.size();
        Iterator keyIterator = keySet.iterator();
        remainingKeys = keySet.size();
        remainingFiles = numberOfFiles;
        //Distribuimos la carga de trabajo de los threads y asignamos los archivos que controla cada uno
        while(remainingKeys>0){
            remainingKeys-=nThreads;
            keysxThread++;
        }
        while(remainingFiles>0){
            remainingFiles-=nThreads;
            filesxThread++;
        }

        //Por cada Hilo que creemos, creamos una instancia de la clase MyThreadI que contendrá el propio thread y sus parametros
        //Le asignamos una lista con las llaves que le tocan y añadimos el thread en otra lista a fin de poder controlar su
        //fin con la funcion join().
        for (int i = 0; i < nThreads; i++) {
            int j = 0;
            ArrayList<String> list = new ArrayList<String>();
            while (j < keysxThread && keyIterator.hasNext()) {
                key = (String) keyIterator.next();
                list.add(key);
                j++;
            }
            MyThreadI t = new MyThreadI(i, list,outputDirectory,(int) filesxThread,Hash, nThreads);
            thr.add(t);
            t.thread.start();
        }
        while(!thr.get(0).sincro){System.out.print("");}
        System.out.println("SALGO");

    }

    // Método para cargar en memoria (HashMap) el índice invertido desde su copia en disco.
    public void LoadIndex(String inputDirectory) {
        HashMultimap<String, Long> newHash = HashMultimap.create();
        long filesxThread = 0, initialfile = 0, finalfile = 0, dif = 0;
        File folder = new File(inputDirectory);
        File[] listOfFiles = folder.listFiles();
        long remainingFiles = listOfFiles.length;
        ArrayList<MyThreadQ> thr = new ArrayList<MyThreadQ>();

       // repartimos el numero de ficheros restantes para cada uno de los threads
        while(remainingFiles>0){
            remainingFiles-=nThreads;
            filesxThread++;
        }
        //Creamos una instancia de la clase MyThreadQ que contendrá el método run y un constructor con los parámetros
        //que utilizara cada thread para cargar en memoria el índice invertido
        for (int i = 0; i < nThreads; i++){
            finalfile = initialfile + filesxThread - 1;
            if(finalfile > listOfFiles.length){
                dif = finalfile - listOfFiles.length;
                finalfile -= dif;
            }
            MyThreadQ t = new MyThreadQ(i, listOfFiles, initialfile, finalfile, Hash,nThreads);
            initialfile += filesxThread;
            thr.add(t); //añadimos el thread al array de threads
            t.thread.start();
        }
        while(!thr.get(0).sincro){System.out.print("");}
        System.out.println("SALGO");

    }

    public void Query(String queryString) {
        String queryResult=null;
        Map<Long, Integer> offsetsFreq, sorted_offsetsFreq;

        System.out.println ("Searching for query: "+queryString);
        // Split Query in keys & Obtain keys offsets
        offsetsFreq = GetQueryOffsets(queryString);
        // Sort offsets by Frequency in descending order
        sorted_offsetsFreq = SortOffsetsFreq(offsetsFreq);
        //PrintOffsetsFreq(sorted_offsetsFreq);
        // Show results (offsets>Threshold)
        try {
            // Open original input file for random access.
            randomInputFile = new RandomAccessFile(InputFilePath, "r");
        } catch (FileNotFoundException e) {
            System.err.println("Error opening input file");
            e.printStackTrace();
        }
        int maxFreq = (queryString.length()-KeySize)+1;
        Iterator<Map.Entry<Long, Integer>> itr = sorted_offsetsFreq.entrySet().iterator();
        while(itr.hasNext())
        {
            Map.Entry<Long, Integer> entry = itr.next();
            // Calculamos el porcentaje de matching y si es superior al mínimo requerido imprimimos el resultado (texto en esta posición del fichero original)
            if (((float)entry.getValue()/(float)maxFreq)>=DMinimunMatchingPercentage) {
                PrintMatching(entry.getKey(), queryString.length(), (float) entry.getValue() / (float) maxFreq);
            } else {
                break;
            }
        }
        try {
            randomInputFile.close();
        } catch (IOException e) {
            System.err.println("Error opening input file");
            e.printStackTrace();
        }
    }

    // Obtenemos un Map con todos la frecuencia de aparicioón de los offssets asociados con las keys (k-words)
    // generadas a partir de la consulta
    private Map<Long, Integer> GetQueryOffsets(String query)
    {

        Map<Long, Integer> offsetsFreq = new HashMap<Long, Integer>();
        int queryLenght = query.length();
        // Recorremos todas las keys (k-words) de la consulta
        for (int k=0;k<=(queryLenght-KeySize); k++)
        {
            String key = query.substring(k, k+KeySize);
            // Obtenemos y procesamos los offsets para esta key.
            for (Long offset : GetKeyOffsets(key))
            {
                // Increase the number of occurrences of the relative offset (offset-k).
                Integer count = offsetsFreq.get(offset-k);
                if (count == null)
                    offsetsFreq.put(offset-k, 1);
                else
                    offsetsFreq.put(offset-k, count + 1);
            }
        }
        return offsetsFreq;
    }

    // Obtenes los offsets asociados con una key
    private Collection<Long> GetKeyOffsets(String key) {
        return Hash.get(key);
    }


    // Ordenamos la frecuencia de aparición de los offsets de mayor a menor
    private Map<Long, Integer> SortOffsetsFreq( Map<Long, Integer> offsetsFreq)
    {
        List<Map.Entry<Long, Integer>> list = new LinkedList<Map.Entry<Long, Integer>>(offsetsFreq.entrySet());

        // Sorting the list based on values
        Collections.sort(list, new Comparator<Map.Entry<Long, Integer>>()
        {
            public int compare(Map.Entry<Long, Integer> o1, Map.Entry<Long, Integer> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });

        // Maintaining insertion order with the help of LinkedList
        Map<Long, Integer> sortedMap = new LinkedHashMap<Long, Integer>();
        for (Map.Entry<Long, Integer> entry : list)
        {
            sortedMap.put(entry.getKey(), entry.getValue());
        }
        return sortedMap;
    }

    // Imprimimos la frecuencia de aparición de los offsets.
    private void PrintOffsetsFreq(Map<Long, Integer> offsetsFreq)
    {
        Iterator<Map.Entry<Long, Integer>> itr = offsetsFreq.entrySet().iterator();
        while(itr.hasNext())
        {
            Map.Entry<Long, Integer> entry = itr.next();
            //System.out.println("Offset " + entry.getKey() + " --> " + entry.getValue());
        }
    }

    // Imprimimos el texto de un matching de la consulta.
    // A partir del offset se lee y se imprime tantos carácteres como el tamaño de la consulta + N caracteres de padding.
    private void PrintMatching(Long offset, int length, float perMatching)
    {
        byte[] matchText = new byte[length+DPaddingMatchText];

        try {
            // Nos posicionamos en el offset deseado.
            randomInputFile.seek(offset.intValue());
            // Leemos el texto.
            randomInputFile.read(matchText);
        } catch (IOException e) {
            System.err.println("Error reading input file");
            e.printStackTrace();
        }
        System.out.println("Matching at offset "+offset+" ("+ perMatching*100 + "%): "+new String(matchText));
    }

}
