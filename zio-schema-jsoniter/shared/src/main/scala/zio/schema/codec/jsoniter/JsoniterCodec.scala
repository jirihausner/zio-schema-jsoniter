package zio.schema.codec.jsoniter

import com.github.plokhotnyuk.jsoniter_scala.core._
import zio.schema._
import zio.schema.codec.jsoniter.internal.{Codecs, JsonSplitter}
import zio.schema.codec.{BinaryCodec, DecodeError}
import zio.stream.ZPipeline
import zio.{Cause, Chunk, ZIO}

object JsoniterCodec {

  final case class Config(
    ignoreEmptyCollections: Boolean,
    ignoreNullValues: Boolean = true,
    treatStreamsAsArrays: Boolean = false,
  )

  object Config {
    val default: Config = Config(ignoreEmptyCollections = false)
  }

  implicit def jsoniterBinaryCodec[A](implicit codec: JsonValueCodec[A]): BinaryCodec[A] = new BinaryCodec[A] {

    override def encode(value: A): Chunk[Byte] = Chunk.fromArray(writeToArray(value))

    override def streamEncoder: ZPipeline[Any, Nothing, A, Byte] =
      ZPipeline.mapChunks[A, Chunk[Byte]](_.map(encode)).intersperse(Chunk.single('\n'.toByte)).flattenChunks

    override def decode(whole: Chunk[Byte]): Either[DecodeError, A] = {
      try Right(readFromArray(whole.toArray))
      catch { case failure: Throwable => Left(DecodeError.ReadError(Cause.fail(failure), failure.getMessage)) }
    }

    override def streamDecoder: ZPipeline[Any, DecodeError, Byte, A] =
      ZPipeline.fromChannel {
        ZPipeline.utf8Decode.channel.mapError(cce => DecodeError.ReadError(Cause.fail(cce), cce.getMessage))
      } >>> JsonSplitter.splitOnJsonBoundary >>> ZPipeline.mapZIO { (json: String) =>
        ZIO.fromEither {
          try Right(readFromString(json))
          catch { case failure: Throwable => Left(DecodeError.ReadError(Cause.fail(failure), failure.getMessage)) }
        }
      }
  }

  def schemaBasedBinaryCodec[A](config: Config)(implicit schema: Schema[A]): BinaryCodec[A] = new BinaryCodec[A] {

    private implicit val codec: JsonValueCodec[A] = schemaCodec(schema)(config)

    override def encode(value: A): Chunk[Byte] = Chunk.fromArray(writeToArray(value))

    override def streamEncoder: ZPipeline[Any, Nothing, A, Byte] =
      if (config.treatStreamsAsArrays) {
        val interspersed: ZPipeline[Any, Nothing, A, Byte] = ZPipeline
          .mapChunks[A, Chunk[Byte]](_.map(encode))
          .intersperse(Chunk.single(','.toByte))
          .flattenChunks
        val prepended: ZPipeline[Any, Nothing, A, Byte]    =
          interspersed >>> ZPipeline.prepend(Chunk.single('['.toByte))
        prepended >>> ZPipeline.append(Chunk.single(']'.toByte))
      } else {
        ZPipeline.mapChunks[A, Chunk[Byte]](_.map(encode)).intersperse(Chunk.single('\n'.toByte)).flattenChunks
      }

    override def decode(whole: Chunk[Byte]): Either[DecodeError, A] = {
      try Right(readFromArray(whole.toArray))
      catch { case failure: Throwable => Left(DecodeError.ReadError(Cause.fail(failure), failure.getMessage)) }
    }

    override def streamDecoder: ZPipeline[Any, DecodeError, Byte, A] =
      ZPipeline.utfDecode.mapError(cce => DecodeError.ReadError(Cause.fail(cce), cce.getMessage)) >>>
        (if (config.treatStreamsAsArrays) JsonSplitter.splitJsonArrayElements
         else JsonSplitter.splitOnJsonBoundary) >>>
        ZPipeline.mapZIO { (json: String) =>
          ZIO.fromEither {
            try Right(readFromString(json))
            catch { case failure: Throwable => Left(DecodeError.ReadError(Cause.fail(failure), failure.getMessage)) }
          }
        }
  }

  def schemaCodec[A](schema: Schema[A])(implicit config: Config = Config.default): JsonValueCodec[A] =
    Codecs.schemaCodec(schema, config)
}
