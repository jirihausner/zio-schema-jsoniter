package zio.schema.codec.jsoniter

import zio._
import zio.schema._
import zio.schema.codec.jsoniter.internal._
import zio.test.TestAspect._
import zio.test._

object JsoniterCodecSpec extends ZIOSpecDefault with EncoderSpecs with DecoderSpecs with EncoderDecoderSpecs {

  override type Config = JsoniterCodec.Config

  override protected def DefaultConfig: JsoniterCodec.Config = JsoniterCodec.Config.default

  override protected def IgnoreEmptyCollectionsConfig: Config       =
    JsoniterCodec.Config(ignoreEmptyCollections = true)
  override protected def KeepNullsAndEmptyColleciontsConfig: Config =
    JsoniterCodec.Config(ignoreEmptyCollections = false, ignoreNullValues = false)
  override protected def StreamingConfig: JsoniterCodec.Config         =
    JsoniterCodec.Config(ignoreEmptyCollections = false, treatStreamsAsArrays = true)

  override protected def BinaryCodec[A]: (Schema[A], Config) => codec.BinaryCodec[A] =
    (schema: Schema[A], config: JsoniterCodec.Config) => JsoniterCodec.schemaBasedBinaryCodec(config)(schema)

  def spec: Spec[TestEnvironment, Any] =
    suite("JsoniterCodec specs")(
      encoderSuite,
      decoderSuite,
      encoderDecoderSuite,
    ) @@ timeout(180.seconds)
}
