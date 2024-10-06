# CS441_Fall2024_HW1

## About Project
    This Project uses Apache Hadoop Map Reduce Framework to process a text corpus.
    The Project uses the Word2Vec model to tokenize and get vector embeddings of each token, and compute 
    embeddings for each token. The embedding is then Aggregated.
    Aggregation can reduce the complexity of handling multiple embeddings, making the data easier to manage and process.
    Noise Reduction: Averaging and other aggregation methods can help mitigate the effect of noise in individual data points, leading to more robust models.
    The solution is then deloyed on Amazon EMR.
     

## Youtube link: https://youtu.be/gfEivvULg74?si=BEJk-iFL1ktz1IZV

### Pre-requisites:
    Hadoop
    Sbt
    Scala
    
### Steps to Run:
    sbt clean 
    sbt compile 
    sbt run <input dir> <output dir>

### To run on Hadoop:
    compile the jar ->
        sbt clean compile
        sbt assemble
        (or use your IDE provided options to build jar)

    hadoop jar <jarfile> <inputdir> <outputdir>