package dagr.core.util

/** Should work just fine. */
class NonCase(i:Int, val l: Long = 42, var s: String) {
  val ii = i
}
/** Should work just fine. */
case class CaseClass(i:Int, l: Long = 42, var s: String)
/** Should work just fine as only a single public constructor */
class MultiConstructor1(val i:Int) {
  private def this(l: Long) = this(l.toInt)
  protected def this(f: Float) = this(f.toInt)
}
/** Should fail due to multiple public constructors */
class MultiConstructor2(val i:Int) {
  def this(l: Long) = this(l.toInt)
}

/**
  * Tests for building classes using reflection
  */
class ReflectiveBuilderTest extends UnitSpec {
  "ReflectiveBuilder" should "work with non-case classes" in {
    val builder = new ReflectiveBuilder(classOf[NonCase])
    builder.argumentLookup.forField("i").get.value = 7
    builder.argumentLookup.forField("s").get.value = "foo"

    val result = builder.build()
    result.ii shouldBe 7
    result.l shouldBe 42
    result.s shouldBe "foo"

    builder.argumentLookup.forField("l").get.value = 43L
    val result2 = builder.build()
    result2.ii shouldBe 7
    result2.l shouldBe 43
    result2.s shouldBe "foo"
  }

  it should "work with case classes" in {
    val builder = new ReflectiveBuilder(classOf[CaseClass])
    builder.argumentLookup.forField("i").get.value = 7
    builder.argumentLookup.forField("s").get.value = "foo"

    val result = builder.build()
    result.i shouldBe 7
    result.l shouldBe 42
    result.s shouldBe "foo"

    builder.argumentLookup.forField("l").get.value = 43L
    val result2 = builder.build()
    result2.i shouldBe 7
    result2.l shouldBe 43
    result2.s shouldBe "foo"
  }

  it should "work with a class with multiple non-public constructors" in {
    val builder = new ReflectiveBuilder(classOf[MultiConstructor1])
    builder.argumentLookup.forField("i").get.value = 7
    val result = builder.build()
    result.i shouldBe 7
  }

  it should "throw an exception when there are multiple public constructors" in {
    an[IllegalArgumentException] should be thrownBy new ReflectiveBuilder(classOf[MultiConstructor2])
  }

  it should "throw an exception if build() is called and some arguments don't have values" in {
    val builder = new ReflectiveBuilder(classOf[CaseClass])
    an[IllegalStateException] should be thrownBy builder.build()
  }
}
