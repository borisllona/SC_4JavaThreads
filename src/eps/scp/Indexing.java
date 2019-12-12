package eps.scp;

import static java.lang.Math.pow;

public class Indexing {

    public static void main(String[] args)
    {
        //sequential(args);
        concurrent(args);
    }
    private static void sequential(String[] args){
        InvertedIndex hash;
        long startTime = System.nanoTime();
        System.out.print("Sequential Version");

        if (args.length <2 || args.length>4)
            System.err.println("Erro in Parameters. Usage: Indexing <TextFile> [<Key_Size>] [<Index_Directory>]");
        if (args.length < 2)
            hash = new InvertedIndex(args[0]);
        else
            hash = new InvertedIndex(args[0], Integer.parseInt(args[1]));

        hash.BuildIndex();

        if (args.length > 2)
            hash.SaveIndex(args[2]);
        else
            hash.PrintIndex();
        long stopTime = System.nanoTime();
        System.out.println("Time: "+(stopTime - startTime)*pow(10,-9));
    }
    private static void concurrent(String[] args){
        InvertedIndexConc hash;
        long startTime = System.nanoTime();

        if (args.length <3 || args.length>5)
            System.err.println("Erro in Parameters. Usage: Indexing <TextFile> [<Key_Size>] [<Index_Directory>]");
        if (args.length < 2)
            hash = new InvertedIndexConc(args[0]);    // Indexing <TextFile>
        else if(args.length < 3)
            hash = new InvertedIndexConc(args[0],Integer.parseInt(args[1])); //Indexing <TextFile> <Number of threads>
        else
            hash = new InvertedIndexConc(args[0],Integer.parseInt(args[1]) ,Integer.parseInt(args[2])); //Indexing <TextFile> <Number of threads> [<Key_Size>]

        hash.BuildIndex();

        if (args.length > 3)
            try {
                hash.SaveIndex(args[3]);
            }catch (InterruptedException e) { e.printStackTrace(); }
        //else
          //  hash.PrintIndex();

        long stopTime = System.nanoTime();
        System.out.println("Time: "+(stopTime - startTime)*pow(10,-9));
    }

}
