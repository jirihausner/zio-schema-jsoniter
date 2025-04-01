package zio.schema.codec.jsoniter

import com.github.plokhotnyuk.jsoniter_scala.core._
import zio.durationInt
import zio.schema.Schema
import zio.test.Assertion.{equalTo, isRight}
import zio.test.TestAspect.timeout
import zio.test._

import scala.util.control.NonFatal

object JsoniterCodecJVMSpec extends ZIOSpecDefault {

  def spec: Spec[TestEnvironment, Any] =
    suite("JsoniterCodec JVM Spec")(
      decoderSuite,
    ) @@ TestAspect.jvmOnly @@ timeout(180.seconds)

  private val decoderSuite = suite("decoding")(
    suite("decodes record with more than 22 fields")(
      test("successfully if missing fields in the json payload are populated with their default values") {
        val exampleSchema = zio.schema.DeriveSchema.gen[RecordExample]
        val string        = """{"f1": "test"}"""
        assertDecodesJson(exampleSchema, RecordExample(Some("test")), string)
      },
      test("with failure if a field with no default value is missing in the json payload") {
        val exampleSchema = zio.schema.DeriveSchema.gen[RecordExample2]
        val string        = """{"f1": "test"}"""
        assertDecodesJsonFailure(exampleSchema, string)
      },
    ),
  )

  private def assertDecodesJson[A](schema: Schema[A], value: A, json: String) = {
    implicit val codec: JsonValueCodec[A] = JsoniterCodec.schemaJsonValueCodec(schema)
    val either                            =
      try Right(readFromString(json))
      catch { case ex if NonFatal(ex) => Left(ex) }
    zio.test.assert(either)(isRight(equalTo(value)))
  }

  private def assertDecodesJsonFailure[A](schema: Schema[A], json: String) = {
    implicit val codec: JsonValueCodec[A] = JsoniterCodec.schemaJsonValueCodec(schema)
    val either                            =
      try Right(readFromString(json))
      catch { case ex if NonFatal(ex) => Left(ex) }
    zio.test.assertTrue(either.isLeft)
  }

  case class RecordExample(
    f1: Option[String],
    f2: Option[String] = None,
    f3: Option[String] = None,
    f4: Option[String] = None,
    f5: Option[String] = None,
    f6: Option[String] = None,
    f7: Option[String] = None,
    f8: Option[String] = None,
    f9: Option[String] = None,
    f10: Option[String] = None,
    f11: Option[String] = None,
    f12: Option[String] = None,
    f13: Option[String] = None,
    f14: Option[String] = None,
    f15: Option[String] = None,
    f16: Option[String] = None,
    f17: Option[String] = None,
    f18: Option[String] = None,
    f19: Option[String] = None,
    f20: Option[String] = None,
    f21: Option[String] = None,
    f22: Option[String] = None,
    f23: Option[String] = None,
  )

  case class RecordExample2(
    f1: Option[String],
    f2: String,
    f3: Option[String] = None,
    f4: Option[String] = None,
    f5: Option[String] = None,
    f6: Option[String] = None,
    f7: Option[String] = None,
    f8: Option[String] = None,
    f9: Option[String] = None,
    f10: Option[String] = None,
    f11: Option[String] = None,
    f12: Option[String] = None,
    f13: Option[String] = None,
    f14: Option[String] = None,
    f15: Option[String] = None,
    f16: Option[String] = None,
    f17: Option[String] = None,
    f18: Option[String] = None,
    f19: Option[String] = None,
    f20: Option[String] = None,
    f21: Option[String] = None,
    f22: Option[String] = None,
    f23: Option[String] = None,
  )
}
