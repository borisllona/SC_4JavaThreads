package eps.scp;

import static java.lang.Math.pow;

/**
 * Created by Nando on 8/10/19.
 */
public class Query
{

    public static void main(String[] args)
    {
        //sequential(args);
        concurrent(args);

    }

    private static void sequential(String[] args) {
        InvertedIndex hash;
        String queryString=null, indexDirectory=null, fileName=null;
        long startTime = System.nanoTime();

        if (args.length <2 || args.length>4)
            System.err.println("Erro in Parameters. Usage: Query <String> <IndexDirectory> <filename> [<Key_Size>]");
        if (args.length > 0)
            queryString = args[0];
        if (args.length > 1)
            indexDirectory = args[1];
        if (args.length > 2)
            fileName = args[2];
        if (args.length > 3)
            hash = new InvertedIndex(Integer.parseInt(args[3]));
        else
            hash = new InvertedIndex();

        hash.LoadIndex(indexDirectory);
        hash.SetFileName(fileName);
        //hash.PrintIndex();
        hash.Query(queryString);
        long stopTime = System.nanoTime();
        System.out.println("Time: "+(stopTime - startTime)*pow(10,-9));
    }
    private static void concurrent(String[] args) {

        InvertedIndexConc hash;
        String queryString=null, indexDirectory=null, fileName=null;
        long startTime = System.nanoTime();

        if (args.length <3 || args.length>5)
            System.err.println("Erro in Parameters. Usage: Query <String> <IndexDirectory> <filename> <numThreads> [<Progress>] [<Key_Size>]");
        if (args.length > 0)
            queryString = args[0];
        if (args.length > 1)
            indexDirectory = args[1];
        if (args.length > 2)
            fileName = args[2];
        if (args.length == 4)
            hash = new InvertedIndexConc(Integer.parseInt(args[3]));
        else if (args.length == 5)
            hash = new InvertedIndexConc(Integer.parseInt(args[3]), Integer.parseInt(args[4]));
        else if (args.length == 6)
            hash = new InvertedIndexConc(Integer.parseInt(args[3]), Integer.parseInt(args[4],Integer.parseInt(args[5])));
        else
            hash = new InvertedIndexConc();
        /*System.out.println("queryString: " + queryString);
        System.out.println("indexdire: " + indexDirectory);
        System.out.println("filename: " + fileName);
        System.out.println("numtrheads: " + hash.getThreads());
        System.out.println("key: " + hash.getKey());*/
        hash.LoadIndex(indexDirectory);
        hash.SetFileName(fileName);
        //hash.PrintIndex();
        hash.Query(queryString);
        long stopTime = System.nanoTime();
        System.out.println("Time: "+(stopTime - startTime)*pow(10,-9));
    }
}
