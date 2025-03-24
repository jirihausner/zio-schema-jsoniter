package zio.schema.codec.jsoniter

import zio.Chunk

import java.nio.CharBuffer
import java.nio.charset.StandardCharsets

package object internal {

  private[internal] def stringify(str: String): String = new String(str.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8)

  private[internal] def charSequenceToByteChunk(chars: CharSequence): Chunk[Byte] = {
    val bytes = StandardCharsets.UTF_8.newEncoder().encode(CharBuffer.wrap(chars))
    Chunk.fromByteBuffer(bytes)
  }
}
