#!/bin/bash

echo "Vaciar directorios Output ejemplo"
rm Output/example1/*
mkdir Output/example1/
rm Output/example2/*
mkdir Output/example2/
rm Output/example3/*
mkdir Output/example3/
rm Output/quijote/*
mkdir Output/quijote/

echo "Ejecutar Indexado"
time java -cp out/artifacts/Indexing_jar/Indexing.jar eps.scp.Indexing test/example1.txt 8 10 10 Output/example1
time java -cp out/artifacts/Indexing_jar/Indexing.jar eps.scp.Indexing test/example2.txt 8 10 10 Output/example2
time java -cp out/artifacts/Indexing_jar/Indexing.jar eps.scp.Indexing test/example3.txt 8 10 10 Output/example3
echo "Indexando Quijote Secuencial"
time java -cp out/artifacts/Indexing_jar/Indexing.jar eps.scp.Indexing test/pg2000.txt 1 10 10 Output/quijote/
echo "Indexando Quijote Concurrente"
time java -cp out/artifacts/Indexing_jar/Indexing.jar eps.scp.Indexing test/pg2000.txt 8 10 10 Output/quijote/
echo "Validar Indices generados"
cat Output/example1/IndexFile* | cut -f2 | tr ',' ' '  | wc -w
cat Output/example2/IndexFile* | cut -f2 | tr ',' ' '  | wc -w
cat Output/example3/IndexFile* | cut -f2 | tr ',' ' '  | wc -w
cat Output/quijote/IndexFile* | cut -f2 | tr ',' ' '  | wc -w

echo "Query Quijote Secuencial"
time java -cp out/artifacts/Indexing_jar/Indexing.jar eps.scp.Query "En un lugar de la Mancha" Output/quijote/ test/pg2000.txt 1 10
echo "Query Quijote Concurrente"
time java -cp out/artifacts/Indexing_jar/Indexing.jar eps.scp.Query "En un lugar de la Mancha" Output/quijote/ test/pg2000.txt 8 10