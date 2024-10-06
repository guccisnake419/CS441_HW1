import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.*
import org.apache.hadoop.mapred.*
import java.io.IOException
import java.util
import scala.jdk.CollectionConverters.*
import org.apache.hadoop.io.Text
import org.apache.hadoop.util.{Tool, ToolRunner}
import org.slf4j.LoggerFactory
import org.deeplearning4j.models.word2vec.Word2Vec
import org.deeplearning4j.text.sentenceiterator.CollectionSentenceIterator
import org.deeplearning4j.text.tokenization.tokenizer.preprocessor.CommonPreprocessor
import org.deeplearning4j.text.tokenization.tokenizerfactory.DefaultTokenizerFactory


object MapReduceProgram extends Configured with Tool {
  private val iterations = 15
  private val maplogger = LoggerFactory.getLogger("MapReduceProgram.Mapper")
  private val reducelogger = LoggerFactory.getLogger("MapReduceProgram.Reducer")

  class TextArrayWritable extends ArrayWritable(classOf[Text]) {
    def this(strings: Array[String]) = {
      this()
      val texts = strings.map(x => new Text(x))
      super.set(texts.asInstanceOf[Array[Writable]])
    }
  }

  def word2VecBuilder(sentence: String): Option[Word2Vec] = {
    val sentenceList = List(sentence).asJava
    val tokenizerFactory = new DefaultTokenizerFactory()
    tokenizerFactory.setTokenPreProcessor(new CommonPreprocessor())
    val sentenceIterator = new CollectionSentenceIterator(sentenceList)
    val word2Vec = new Word2Vec.Builder()
      .minWordFrequency(1)
      .tokenizerFactory(tokenizerFactory)
      .iterate(sentenceIterator)
      .iterations(iterations)
      .layerSize(100)
      .windowSize(5)
      .seed(42)
      .build()
    maplogger.info("Training Word2Vec model...")
    try {
      word2Vec.fit()
    } catch {
      case p: IllegalStateException => {
        maplogger.error(p.getMessage + " for sentence: " + sentence)
        return None
      }
    }

    maplogger.info("Model training complete.")
    Option(word2Vec)
  }

  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, TextArrayWritable] {
    private val word = new Text()


    @throws[IOException]
    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, TextArrayWritable], reporter: Reporter): Unit = {
      maplogger.info("Starting Map job for key: " + key.toString)
      val line: String = value.toString
      if (line.equals(" ") || line.equals("")) {
        return
      }
      val vec = word2VecBuilder(line)
      if (vec == None) {
        return
      }
      val vocab = vec.get.vocab().words().asScala
      vocab.foreach(token =>{
        val vector = vec.get.getWordVector(token) ++ Array(1.0)
        word.set(token)
        output.collect(word, new TextArrayWritable(vector.map(x => x.toString)))}
      )
    }
  }

  class Reduce extends MapReduceBase with Reducer[Text, TextArrayWritable, Text, Text] {
    override def reduce(key: Text, values: util.Iterator[TextArrayWritable], output: OutputCollector[Text, Text], reporter: Reporter): Unit = {
      reducelogger.info("Starting Reduce job for key: " + key.toString)
      val embeddings = values.asScala.toList
      val summedEmbedding = embeddings.map(_.get().map(_.toString.toDouble).init)
        .reduce((vec1, vec2) => vec1.zip(vec2).map { case (v1, v2) => v1 + v2 })
      val totalFrequency = embeddings.map(_.get().last.toString.toDouble).sum
      val averagedEmbedding = summedEmbedding.map(_ / totalFrequency)
      val resultArray = averagedEmbedding.map(_.toString) ++ Array(totalFrequency.toString)
      output.collect(key, new Text("Average Embeddings: " + averagedEmbedding.map(x => x.toString).mkString("Array(", ", ", ")") + "   \nCount: " + totalFrequency))

    }
  }

  override def run(args: Array[String]): Int =
    if (args.length != 2) {
      println("Usage: MapReduceProgram <input path> <output path>")
      return -1
    }
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("TokenCount")
    conf.set("fs.defaultFS", "local")
    conf.set("mapreduce.job.maps", "1")
    conf.set("mapreduce.job.reduces", "1")
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[TextArrayWritable])
    conf.setMapperClass(classOf[Map])
    conf.setReducerClass(classOf[Reduce])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, TextArrayWritable]])
    FileInputFormat.setInputPaths(conf, new Path(args(0)))
    FileOutputFormat.setOutputPath(conf, new Path(args(1)))
    JobClient.runJob(conf)
    0

  def main(args: Array[String]): Unit = {
    val exitCode = ToolRunner.run(new Configuration(), this, args)
    System.exit(exitCode)
  }
}