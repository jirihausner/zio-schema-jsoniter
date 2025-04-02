package zio.schema.codec.jsoniter

import com.github.plokhotnyuk.jsoniter_scala.core._
import zio.Console.*
import zio.*
import zio.schema.*
import zio.schema.annotation.*
import zio.test.Assertion.*
import zio.test.TestAspect.*
import zio.test.{assert, assertTrue, Spec, TestEnvironment, ZIOSpecDefault}
import scala.util.Try

object VersionSpecificJsoniterCodecSpec extends VersionSpecificCodecSpec {

  override protected def schemaCodec[A: Schema]: JsonValueCodec[A] =
    JsoniterCodec.schemaJsonValueCodec(Schema[A])

  def spec: Spec[TestEnvironment, Any] =
    suite("VersionSpecificJsoniterCodecSpec")(
      customSuite
    ) @@ timeout(90.seconds)
}

trait VersionSpecificCodecSpec extends ZIOSpecDefault  {

  protected def schemaCodec[A: Schema]: JsonValueCodec[A]

  protected val customSuite = suite("custom")(
    suite("default value schema")(
      test("default value at last field") {
        val result =
          Try(readFromString("""{"orderId": 1}""")(using schemaCodec(using Schema[WithDefaultValue])))
        assertTrue(result.toEither.isRight)
      }
    ),
    suite("enum with discrimintator")(
      test("default value at last field") {
        val value = BaseB("a", Inner(1))
        val json = """{"type":"BaseB","a":"a","b":{"i":1}}"""
        assert(readFromString(json)(using schemaCodec(using Schema[Base])))(equalTo(value)) &&
        assert(writeToString(value)(using schemaCodec(using Schema[Base])))(equalTo(json))
      }
    ),
    suite("union types")(
      test("union type of standard types") {
        val schema = Schema.chunk(DeriveSchema.gen[Int | String | Boolean])
        val json = """["abc",1,true]"""
        val value = Chunk[Int | String | Boolean]("abc", 1, true)
        assert(readFromString(json)(using schemaCodec(using schema)))(equalTo(value)) &&
        assert(writeToString(value)(using schemaCodec(using schema)))(equalTo(json))
      },
      test("union type of enums") {
        val schema = Schema.chunk(Schema[Result])
        val json = """[{"res":{"Left":"Err1"}},{"res":{"Left":"Err21"}},{"res":{"Right":{"i":1}}}]"""
        val value = Chunk[Result](Result(Left(ErrorGroup1.Err1)), Result(Left(ErrorGroup2.Err21)), Result(Right(Value(1))))
        assert(readFromString(json)(using schemaCodec(using schema)))(equalTo(value)) &&
        assert(writeToString(value)(using schemaCodec(using schema)))(equalTo(json))
      },
      test("union type of custom types") {
        import UnionValue.given

        val schema = Schema.map(Schema[String], Schema[UnionValue])
        val json = """{"a":1,"b":"toto","c":true,"d":null}"""
        val value = Map("a" -> 1, "b" -> "toto", "c" -> true, "d" -> null)
        assert(readFromString(json)(using schemaCodec(using schema)))(equalTo(value)) &&
        assert(writeToString(value)(using schemaCodec(using schema)))(equalTo(json))
      }
    )
  )

  case class WithDefaultValue(orderId: Int, description: String = "desc")

  object WithDefaultValue {
    implicit lazy val schema: Schema[WithDefaultValue] = DeriveSchema.gen[WithDefaultValue]
  }

  enum ErrorGroup1:
    case Err1
    case Err2
    case Err3

  enum ErrorGroup2:
    case Err21
    case Err22
    case Err23

  case class Value(i: Int)
  object Value:
    given Schema[Value] = DeriveSchema.gen[Value]

  case class Result(res: Either[ErrorGroup1 | ErrorGroup2, Value])
  object Result:
    given Schema[Result] = DeriveSchema.gen[Result]

  case class Inner(i: Int) derives Schema

  @discriminatorName("type")
  sealed trait Base derives Schema:
    def a: String

  case class BaseA(a: String) extends Base derives Schema

  case class BaseB(a: String, b: Inner) extends Base derives Schema

  given Schema[Null] = Schema.option[Unit].transform[Null]({ _ => null }, { _ => None })

  type UnionValue = Int | Boolean | String | Null

  object UnionValue {
    given Schema[UnionValue] = Schema.enumeration[UnionValue, CaseSet.Aux[UnionValue]](
      TypeId.Structural,
      CaseSet.caseOf[Int, UnionValue]("int")(_.asInstanceOf[Int])(_.asInstanceOf[UnionValue])(_.isInstanceOf[Int]) ++
        CaseSet.caseOf[Boolean, UnionValue]("boolean")(_.asInstanceOf[Boolean])(_.asInstanceOf[UnionValue])(_.isInstanceOf[Boolean]) ++
        CaseSet.caseOf[String, UnionValue]("string")(_.asInstanceOf[String])(_.asInstanceOf[UnionValue])(_.isInstanceOf[String]) ++
        CaseSet.caseOf[Null, UnionValue]("null")(_.asInstanceOf[Null])(_.asInstanceOf[UnionValue])(_ == null),
      Chunk(noDiscriminator())
    )
  }
}
