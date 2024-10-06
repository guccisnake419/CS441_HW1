import org.scalatest.funsuite.AnyFunSuite

class MapReduceProgramTest extends AnyFunSuite {


  test("An empty Set should have size 0") {
    assert(Set.empty.size == 0)
  }
  test("Testing word2VecBuilder Execption/Bad case"){
    assert(MapReduceProgram.word2VecBuilder("1800")== None)
  }
  test("Testing word2VecBuilder with regular input"){
    assert(MapReduceProgram.word2VecBuilder("Stream Everything you heard is true by Odunsi the Engine")!= None)
  }
  test("Testing new constructor in TextArrayWritable"){
    val shape = new MapReduceProgram.TextArrayWritable(Array("Fela Anikulapo Kuti", "Beautiful Son of Africa"))
    assert(shape.get().toArray.length ==2)
  } 

  test("word2VecBuilder should return None for null input") {
    val result = MapReduceProgram.word2VecBuilder(null)
    assert(result.isEmpty, "The result should be None for null input")
  }
  test("word2VecBuilder should handle short sentences") {
    val shortSentence = "Hi."

    val result = MapReduceProgram.word2VecBuilder(shortSentence)
    assert(result.isDefined, "The result should be Some(Word2Vec) for a short sentence")
  }

  test("Invoking head on an empty Set should produce NoSuchElementException") {
    assertThrows[NoSuchElementException] {
      Set.empty.head
    }
  }
}