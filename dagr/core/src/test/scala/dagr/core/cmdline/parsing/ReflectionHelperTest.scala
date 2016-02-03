package dagr.core.cmdline.parsing

import java.util

import dagr.core.cmdline.{CLPConstructor, Arg}
import dagr.core.util.UnitSpec

/**
  *
  */
private case class IntNoDefault(@Arg var v: Int)
private case class TwoParamNoDefault(@Arg var v: Int = 2, @Arg var w: Int)
private case class IntDefault(@Arg var v: Int = 2)
private case class DefaultWithOption(@Arg var w: Option[Int] = None)
private case class NoParams()
private case class StringDefault(@Arg var s: String = "null")
private case class ComplexDefault(@Arg var v: StringDefault = new StringDefault())
private case class ComplexNoDefault(@Arg var v: StringDefault)

private class NotCaseClass(@Arg val i:Int, @Arg val l:Long=123, @Arg var o : Option[String] = None)
private class ParamsNotVals(@Arg i:Int, @Arg l:Long) {
  val x = i
  val y = l
}

private class SecondaryConstructor1(val x:Int, val y:Int) { def this(@Arg a:Int) = this(a, a*2) }
private class SecondaryConstructor2(val x:Int, val y:Int) { @CLPConstructor def this() = this(2, 4) }

class ReflectionHelperTest extends UnitSpec {

  "ReflectionHelper" should "instantiate a case-class with defaults" in {
    val t = new ReflectionHelper(classOf[IntDefault]).buildDefault()
    t.v should be (2)
    val tt = new ReflectionHelper(classOf[DefaultWithOption]).buildDefault()
    tt.w should be (None)
    new ReflectionHelper(classOf[NoParams]).buildDefault()
    new ReflectionHelper(classOf[ComplexDefault]).buildDefault()
  }

  it should "instantiate a case-class with arguments" in {
    val t = new ReflectionHelper(classOf[IntDefault]).build(List(3))
    t.v shouldBe 3
    val tt = new ReflectionHelper(classOf[DefaultWithOption]).build(List(None))
    tt.w shouldBe 'empty
    new ReflectionHelper(classOf[NoParams]).build(Nil)
    new ReflectionHelper(classOf[ComplexDefault]).build(List(new StringDefault()))
  }

  it should "throw an exception when arguments are missing when trying to instantiate a case-class" in {
    an[IllegalArgumentException] should be thrownBy new ReflectionHelper(classOf[ComplexDefault]).build(Nil)
  }

  it should "work with non-case classes" in {
    val t = new ReflectionHelper(classOf[NotCaseClass]).build(Seq(12, 456.asInstanceOf[Long], Option("Hello")))
    t.i shouldBe 12
    t.l shouldBe 456
    t.o shouldBe Some("Hello")
  }

  it should "work with constructor parameters that are not vals or vars " in {
    val t = new ReflectionHelper(classOf[ParamsNotVals]).build(Seq(911, 999))
    t.x shouldBe 911
    t.y shouldBe 999
  }

  it should "work with a secondary constructor with @Arg annotations in it " in {
    val t = new ReflectionHelper(classOf[SecondaryConstructor1]).build(Seq(50))
    t.x shouldBe 50
    t.y shouldBe 100
  }

  it should "work with a secondary constructor with a @CLPConstructor annotation on it " in {
    val t = new ReflectionHelper(classOf[SecondaryConstructor2]).build(Nil)
    t.x shouldBe 2
    t.y shouldBe 4
  }
}
