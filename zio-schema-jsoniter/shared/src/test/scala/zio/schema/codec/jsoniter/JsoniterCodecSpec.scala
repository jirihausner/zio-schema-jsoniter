package zio.schema.codec.jsoniter

import zio._
import zio.schema._
import zio.schema.codec.jsoniter.JsoniterCodec.{Configuration, ExplicitConfig}
import zio.schema.codec.jsoniter.internal._
import zio.test.TestAspect._
import zio.test._

object JsoniterCodecSpec extends ZIOSpecDefault with EncoderSpecs with DecoderSpecs with EncoderDecoderSpecs {

  override protected def IgnoreEmptyCollectionsConfig: Configuration       =
    Configuration.default.withEmptyCollectionsIgnored.withNullValuesIgnored
  override protected def KeepNullsAndEmptyColleciontsConfig: Configuration =
    Configuration.default.copy(
      explicitEmptyCollections = ExplicitConfig(decoding = true),
      explicitNullValues = ExplicitConfig(decoding = true),
    )
  override protected def StreamingConfig: Configuration                    =
    Configuration.default.copy(treatStreamsAsArrays = true)

  override protected def BinaryCodec[A]: (Schema[A], Configuration) => codec.BinaryCodec[A] =
    (schema: Schema[A], config: Configuration) => JsoniterCodec.schemaBasedBinaryCodec(config)(schema)

  def spec: Spec[TestEnvironment, Any] =
    suite("JsoniterCodec specs")(
      encoderSuite,
      decoderSuite,
      encoderDecoderSuite,
    ) @@ timeout(180.seconds)
}
