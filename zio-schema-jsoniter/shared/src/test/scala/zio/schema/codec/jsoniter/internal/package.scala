package zio.schema.codec.jsoniter

import com.github.plokhotnyuk.jsoniter_scala.core._
import zio.Chunk

import java.nio.CharBuffer
import java.nio.charset.StandardCharsets

package object internal {

  private implicit val stringCodec: JsonValueCodec[String] = new JsonValueCodec[String] {
    def decodeValue(in: JsonReader, default: String): String = in.readString(default)
    def encodeValue(x: String, out: JsonWriter): Unit        = out.writeVal(x)
    def nullValue: String                                    = null
  }

  private[internal] def stringify(str: String): String = writeToString(str)

  private[internal] def charSequenceToByteChunk(chars: CharSequence): Chunk[Byte] = {
    val bytes = StandardCharsets.UTF_8.newEncoder().encode(CharBuffer.wrap(chars))
    Chunk.fromByteBuffer(bytes)
  }
}
