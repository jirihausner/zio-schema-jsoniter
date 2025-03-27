package zio.schema.codec.jsoniter.internal

import com.github.plokhotnyuk.jsoniter_scala.core._
import zio.prelude.NonEmptyMap
import zio.schema._
import zio.schema.annotation._
import zio.schema.codec.jsoniter.JsoniterCodec
import zio.{Chunk, ChunkBuilder}

import java.util.concurrent.ConcurrentHashMap
import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.util.control.NonFatal

private[jsoniter] object Codecs extends Codecs

private[jsoniter] trait Codecs {

  trait Encoder[A] {
    def apply(value: A, out: JsonWriter): Unit
  }
  trait Decoder[A] {
    def apply(in: JsonReader, default: A): A
  }
  type KeyEncoder[A] = (A, JsonWriter) => Unit
  type KeyDecoder[A] = (JsonReader) => A

  type DiscriminatorTuple = Option[(String, String)]

  def encodeChunk[A](implicit encoder: Encoder[A]): Encoder[Chunk[A]] =
    (a: Chunk[A], out: JsonWriter) => {
      out.writeArrayStart()
      val it: Iterator[A] = a.iterator
      while (it.hasNext) encoder(it.next(), out)
      out.writeArrayEnd()
    }

  def decodeChunk[A](implicit decoder: Decoder[A]): Decoder[Chunk[A]] =
    (in: JsonReader, _: Chunk[A]) => {
      if (!in.isNextToken('[')) in.decodeError("expected '['")
      if (in.isNextToken(']')) Chunk.empty[A]
      else {
        in.rollbackToken()
        val builder = ChunkBuilder.make[A](8)
        while ({
          builder += decoder(in, null.asInstanceOf[A])
          in.isNextToken(',')
        }) ()
        if (in.isCurrentToken(']')) builder.result()
        else in.arrayEndOrCommaError()
      }
    }

  def encodePrimitive[A](standardType: StandardType[A]): Encoder[A] = standardType match {
    case StandardType.UnitType           =>
      (_, out) => { out.writeObjectStart(); out.writeObjectEnd() }
    case StandardType.StringType         => (a, out) => out.writeVal(a)
    case StandardType.BoolType           => (a, out) => out.writeVal(a)
    case StandardType.ByteType           => (a, out) => out.writeVal(a)
    case StandardType.ShortType          => (a, out) => out.writeVal(a)
    case StandardType.IntType            => (a, out) => out.writeVal(a)
    case StandardType.LongType           => (a, out) => out.writeVal(a)
    case StandardType.FloatType          => (a, out) => out.writeVal(a)
    case StandardType.DoubleType         => (a, out) => out.writeVal(a)
    case StandardType.BinaryType         =>
      (xs, out) => encodeChunk[Byte] { (a, out) => out.writeVal(a) }(xs, out)
    case StandardType.CharType           => (a, out) => out.writeVal(a)
    case StandardType.BigIntegerType     => (a, out) => out.writeVal(BigInt(a))
    case StandardType.BigDecimalType     => (a, out) => out.writeVal(BigDecimal(a))
    case StandardType.UUIDType           => (a, out) => out.writeVal(a)
    case StandardType.DayOfWeekType      => (a, out) => out.writeVal(a.toString)
    case StandardType.DurationType       => (a, out) => out.writeVal(a)
    case StandardType.InstantType        => (a, out) => out.writeVal(a)
    case StandardType.LocalDateType      => (a, out) => out.writeVal(a)
    case StandardType.LocalDateTimeType  => (a, out) => out.writeVal(a)
    case StandardType.LocalTimeType      => (a, out) => out.writeVal(a)
    case StandardType.MonthType          => (a, out) => out.writeVal(a.toString)
    case StandardType.MonthDayType       => (a, out) => out.writeVal(a)
    case StandardType.OffsetDateTimeType => (a, out) => out.writeVal(a)
    case StandardType.OffsetTimeType     => (a, out) => out.writeVal(a)
    case StandardType.PeriodType         => (a, out) => out.writeVal(a)
    case StandardType.YearType           => (a, out) => out.writeVal(a)
    case StandardType.YearMonthType      => (a, out) => out.writeVal(a)
    case StandardType.ZonedDateTimeType  => (a, out) => out.writeVal(a)
    case StandardType.ZoneIdType         => (a, out) => out.writeVal(a)
    case StandardType.ZoneOffsetType     => (a, out) => out.writeVal(a)
    case StandardType.CurrencyType       => (a, out) => out.writeVal(a.getCurrencyCode)
  }

  def decodePrimitive[A](standardType: StandardType[A]): Decoder[A] = standardType match {
    case StandardType.UnitType           =>
      (in, _) =>
        in.nextToken() match {
          case '{' => if (in.isNextToken('}')) () else in.decodeError("expected '}'")
          case '[' => if (in.isNextToken(']')) () else in.decodeError("expected ']'")
          case _   => in.readNullOrError((), "expected '{' or '[' or null")
        }
    case StandardType.StringType         => (in, _) => in.readString(null)
    case StandardType.BoolType           => (in, _) => in.readBoolean()
    case StandardType.ByteType           => (in, _) => in.readByte()
    case StandardType.ShortType          => (in, _) => in.readShort()
    case StandardType.IntType            => (in, _) => in.readInt()
    case StandardType.LongType           => (in, _) => in.readLong()
    case StandardType.FloatType          => (in, _) => in.readFloat()
    case StandardType.DoubleType         => (in, _) => in.readDouble()
    case StandardType.BinaryType         =>
      new Decoder[Chunk[Byte]] {
        val decoder = decodePrimitive(StandardType[Byte])

        def apply(in: JsonReader, default: Chunk[Byte]): Chunk[Byte] =
          decodeChunk[Byte](decoder)(in, null)
      }
    case StandardType.CharType           => (in, _) => in.readChar()
    case StandardType.BigIntegerType     => (in, _) => in.readBigInt(null).underlying
    case StandardType.BigDecimalType     => (in, _) => in.readBigDecimal(null).underlying
    case StandardType.UUIDType           => (in, _) => in.readUUID(null)
    case StandardType.DayOfWeekType      =>
      (in, _) => java.time.DayOfWeek.valueOf(in.readString(null))
    case StandardType.DurationType       => (in, _) => in.readDuration(null)
    case StandardType.InstantType        => (in, _) => in.readInstant(null)
    case StandardType.LocalDateType      => (in, _) => in.readLocalDate(null)
    case StandardType.LocalDateTimeType  => (in, _) => in.readLocalDateTime(null)
    case StandardType.LocalTimeType      => (in, _) => in.readLocalTime(null)
    case StandardType.MonthType          => (in, _) => java.time.Month.valueOf(in.readString(null))
    case StandardType.MonthDayType       => (in, _) => in.readMonthDay(null)
    case StandardType.OffsetDateTimeType => (in, _) => in.readOffsetDateTime(null)
    case StandardType.OffsetTimeType     => (in, _) => in.readOffsetTime(null)
    case StandardType.PeriodType         => (in, _) => in.readPeriod(null)
    case StandardType.YearType           => (in, _) => in.readYear(null)
    case StandardType.YearMonthType      => (in, _) => in.readYearMonth(null)
    case StandardType.ZonedDateTimeType  => (in, _) => in.readZonedDateTime(null)
    case StandardType.ZoneIdType         => (in, _) => in.readZoneId(null)
    case StandardType.ZoneOffsetType     => (in, _) => in.readZoneOffset(null)
    case StandardType.CurrencyType       =>
      (in, _) => java.util.Currency.getInstance(in.readString(null))
  }

  private case class EncoderKey[A](
    schema: Schema[A],
    config: JsoniterCodec.Config,
    discriminatorTuple: DiscriminatorTuple,
  ) {
    override val hashCode: Int = System.identityHashCode(schema) ^ config.hashCode ^ discriminatorTuple.hashCode
    override def equals(obj: Any): Boolean = obj match {
      case x: EncoderKey[_] => (x.schema eq schema) && x.config == config && x.discriminatorTuple == discriminatorTuple
      case _                => false
    }
  }

  private case class DecoderKey[A](schema: Schema[A], discriminator: Option[String]) {
    override val hashCode: Int             = System.identityHashCode(schema) ^ discriminator.hashCode
    override def equals(obj: Any): Boolean = obj match {
      case x: DecoderKey[_] => (x.schema eq schema) && x.discriminator == discriminator
      case _                => false
    }
  }

  private val encoders = new ConcurrentHashMap[EncoderKey[_], Encoder[_]]()
  private val decoders = new ConcurrentHashMap[DecoderKey[_], Decoder[_]]()

  def encodeSchema[A](
    schema: Schema[A],
    config: JsoniterCodec.Config,
    discriminatorTuple: DiscriminatorTuple = None,
  ): Encoder[A] = {
    val key                 = EncoderKey(schema, config, discriminatorTuple)
    var encoder: Encoder[A] = encoders.get(key).asInstanceOf[Encoder[A]]
    if (encoder eq null) {
      encoder = encodeSchemaSlow(schema, config, discriminatorTuple)
      encoders.put(key, encoder)
    }
    encoder
  }

  def decodeSchema[A](schema: Schema[A], discriminator: Option[String] = None): Decoder[A] = {
    val key     = DecoderKey(schema, discriminator)
    var decoder = decoders.get(key).asInstanceOf[Decoder[A]]
    if (decoder eq null) {
      decoder = decodeSchemaSlow(schema, discriminator)
      decoders.put(key, decoder)
    }
    decoder
  }

  def encodeSchemaSlow[A](
    schema: Schema[A],
    config: JsoniterCodec.Config,
    discriminatorTuple: DiscriminatorTuple = None,
  ): Encoder[A] = schema match {
    case Schema.Primitive(standardType, _)           => encodePrimitive(standardType)
    case Schema.Optional(schema, _)                  => encodeOption(schema, config)
    case Schema.Tuple2(l, r, _)                      => encodeTuple(l, r, config)
    case Schema.Sequence(schema, _, g, _, _)         =>
      new Encoder[A] {
        val writeChunk = encodeChunk(encodeSchema(schema, config))

        def apply(value: A, out: JsonWriter): Unit =
          writeChunk(g(value), out)
      }
    case Schema.NonEmptySequence(schema, _, g, _, _) =>
      new Encoder[A] {
        val writeChunk = encodeChunk(encodeSchema(schema, config))

        def apply(value: A, out: JsonWriter): Unit =
          writeChunk(g(value), out)
      }
    case Schema.Map(ks, vs, _)                       => encodeMap(ks, vs, config)
    case Schema.NonEmptyMap(ks, vs, _)               => encodeNonEmptyMap(ks, vs, config)
    case s @ Schema.Set(schema, _)                   =>
      new Encoder[A] {
        val writeChunk = encodeChunk(encodeSchema(schema, config))

        def apply(value: A, out: JsonWriter): Unit =
          writeChunk(s.toChunk(value), out)
      }
    case Schema.Transform(c, _, g, an, _)            =>
      new Encoder[A] {
        val writeValue = encodeSchema(an.foldLeft(c)((s, a) => s.annotate(a)), config, discriminatorTuple)

        def apply(value: A, out: JsonWriter): Unit = g(value) match {
          case Left(_)      => out.writeNull()
          case Right(value) => writeValue(value, out)
        }
      }
    case Schema.Fail(_, _)                           =>
      (_, out) => { out.writeObjectStart(); out.writeObjectEnd() }
    case Schema.Either(left, right, _)               => encodeEither(left, right, config)
    case Schema.Fallback(left, right, _, _)          => encodeFallback(left, right, config)
    case s: Schema.Lazy[A]                           =>
      new Encoder[A] {
        val writeLazy = () => encodeSchema(s.schema, config, discriminatorTuple)

        def apply(value: A, out: JsonWriter): Unit = writeLazy()(value, out)
      }
    case s: Schema.GenericRecord                     => encodeRecord(s, config, discriminatorTuple)
    case s: Schema.Record[A]                         => encodeCaseClass(s, config, discriminatorTuple)
    case s: Schema.Enum[A]                           => encodeEnum(s, config)
    case s: Schema.Dynamic                           => encodeDynamic(s, config)
    case null                                        =>
      throw new Exception(s"A captured schema is null, most likely due to wrong field initialization order")
  }

  def decodeSchemaSlow[A](schema: Schema[A], discriminator: Option[String] = None): Decoder[A] = schema match {
    case Schema.Primitive(standardType, _)                                   => decodePrimitive(standardType)
    case Schema.Optional(schema, _)                                          => decodeOption(schema)
    case Schema.Tuple2(left, right, _)                                       => decodeTuple(left, right)
    case s @ Schema.Sequence(_, _, _, _, _)                                  => decodeSeq(s)
    case s @ Schema.NonEmptySequence(_, _, _, _, _)                          => decodeNonEmptySeq(s)
    case Schema.Map(ks, vs, _)                                               => decodeMap(ks, vs)
    case Schema.NonEmptyMap(ks, vs, _)                                       => decodeNonEmptyMap(ks, vs)
    case s @ Schema.Set(_, _)                                                => decodeSet(s)
    case s @ Schema.Transform(_, _, _, _, _)                                 => decodeTransform(s, discriminator)
    case Schema.Fail(message, _)                                             => (in, _) => in.decodeError(message)
    case Schema.Either(left, right, _)                                       => decodeEither(left, right)
    case Schema.Fallback(left, right, fullDecode, _)                         => decodeFallback(left, right, fullDecode)
    case s: Schema.Lazy[_]                                                   =>
      new Decoder[A] {
        val readLazy = () => decodeSchema(s.schema, discriminator)

        def apply(in: JsonReader, default: A): A = readLazy()(in, null.asInstanceOf[A])
      }
    case s: Schema.GenericRecord                                             => decodeRecord(s, discriminator)
    case s @ Schema.CaseClass0(_, _, _)                                      => decodeCaseClass0(s)
    case s @ Schema.CaseClass1(_, _, _, _)                                   => decodeCaseClass1(s, discriminator)
    case s @ Schema.CaseClass2(_, _, _, _, _)                                => decodeCaseClass2(s, discriminator)
    case s @ Schema.CaseClass3(_, _, _, _, _, _)                             => decodeCaseClass3(s, discriminator)
    case s @ Schema.CaseClass4(_, _, _, _, _, _, _)                          => decodeCaseClass4(s, discriminator)
    case s @ Schema.CaseClass5(_, _, _, _, _, _, _, _)                       => decodeCaseClass5(s, discriminator)
    case s @ Schema.CaseClass6(_, _, _, _, _, _, _, _, _)                    => decodeCaseClass6(s, discriminator)
    case s @ Schema.CaseClass7(_, _, _, _, _, _, _, _, _, _)                 => decodeCaseClass7(s, discriminator)
    case s @ Schema.CaseClass8(_, _, _, _, _, _, _, _, _, _, _)              => decodeCaseClass8(s, discriminator)
    case s @ Schema.CaseClass9(_, _, _, _, _, _, _, _, _, _, _, _)           => decodeCaseClass9(s, discriminator)
    case s @ Schema.CaseClass10(_, _, _, _, _, _, _, _, _, _, _, _, _)       => decodeCaseClass10(s, discriminator)
    case s @ Schema.CaseClass11(_, _, _, _, _, _, _, _, _, _, _, _, _, _)    =>
      decodeCaseClass11(s, discriminator)
    case s @ Schema.CaseClass12(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
      decodeCaseClass12(s, discriminator)
    case s @ Schema.CaseClass13(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)                   =>
      decodeCaseClass13(s, discriminator)
    case s @ Schema
          .CaseClass14(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
      decodeCaseClass14(s, discriminator)
    case s @ Schema
          .CaseClass15(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
      decodeCaseClass15(s, discriminator)
    case s @ Schema.CaseClass16(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)          =>
      decodeCaseClass16(s, discriminator)
    case s @ Schema.CaseClass17(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)       =>
      decodeCaseClass17(s, discriminator)
    case s @ Schema.CaseClass18(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)    =>
      decodeCaseClass18(s, discriminator)
    case s @ Schema.CaseClass19(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
      decodeCaseClass19(s, discriminator)
    case s @ Schema.CaseClass20(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
      decodeCaseClass20(s, discriminator)
    case s @ Schema.CaseClass21(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
      decodeCaseClass21(s, discriminator)
    case s @ Schema.CaseClass22(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
      decodeCaseClass22(s, discriminator)
    case s: Schema.Enum[A]                                                                        => decodeEnum(s)
    case s: Schema.Dynamic                                                                        => decodeDynamic(s)
    case _ => throw new Exception(s"Missing a handler for decoding of schema ${schema.toString()}.")
  }

  def encodeOption[A](schema: Schema[A], config: JsoniterCodec.Config): Encoder[Option[A]] =
    new Encoder[Option[A]] {
      val writeValue = encodeSchema(schema, config)

      def apply(value: Option[A], out: JsonWriter): Unit = value match {
        case Some(value) => writeValue(value, out)
        case None        => out.writeNull()
      }
    }

  def decodeOption[A](schema: Schema[A]): Decoder[Option[A]] = {
    if (schema.isInstanceOf[Schema.Record[_]] || schema.isInstanceOf[Schema.Enum[_]]) {
      new Decoder[Option[A]] {
        val readValue = decodeSchema(schema)

        def apply(in: JsonReader, default: Option[A]): Option[A] = in.nextToken() match {
          case 'n' => in.readNullOrError(None, "expected null")
          case '{' =>
            if (!in.isNextToken('}')) {
              in.rollbackToken()
              in.rollbackToken()
              Some(readValue(in, null.asInstanceOf[A]))
            } else None
          case _   =>
            in.rollbackToken()
            Some(readValue(in, null.asInstanceOf[A]))
        }
      }
    } else {
      new Decoder[Option[A]] {
        val readValue = decodeSchema(schema)

        def apply(in: JsonReader, default: Option[A]): Option[A] = in.nextToken() match {
          case 'n' => in.readNullOrError(None, "expected null")
          case _   =>
            in.rollbackToken()
            Some(readValue(in, null.asInstanceOf[A]))
        }
      }

    }
  }

  def encodeTuple[A, B](left: Schema[A], right: Schema[B], config: JsoniterCodec.Config): Encoder[(A, B)] =
    new Encoder[(A, B)] {
      val writeLeft  = encodeSchema(left, config)
      val writeRight = encodeSchema(right, config)

      def apply(value: (A, B), out: JsonWriter): Unit = {
        out.writeArrayStart()
        writeLeft(value._1, out)
        writeRight(value._2, out)
        out.writeArrayEnd()
      }
    }

  def decodeTuple[A, B](left: Schema[A], right: Schema[B]): Decoder[(A, B)] =
    new Decoder[(A, B)] {
      val readLeft  = decodeSchema(left)
      val readRight = decodeSchema(right)

      def apply(in: JsonReader, default: (A, B)): (A, B) = {
        if (!in.isNextToken('[')) in.decodeError("expected '['")
        val a = readLeft(in, null.asInstanceOf[A])
        if (!in.isNextToken(',')) in.commaError()
        val b = readRight(in, null.asInstanceOf[B])
        if (!in.isNextToken(']')) in.decodeError("expected ']'")
        else (a, b)
      }
    }

  def encodeField[B](schema: Schema[B]): Option[Encoder[B]] = schema match {
    case Schema.Primitive(StandardType.StringType, _) => Some((a, out) => out.writeKey(a))
    case Schema.Primitive(StandardType.CharType, _)   => Some((a, out) => out.writeKey(a))
    case Schema.Primitive(StandardType.BoolType, _)   => Some((a, out) => out.writeKey(a))
    case Schema.Primitive(StandardType.ByteType, _)   => Some((a, out) => out.writeKey(a))
    case Schema.Primitive(StandardType.ShortType, _)  => Some((a, out) => out.writeKey(a))
    case Schema.Primitive(StandardType.IntType, _)    => Some((a, out) => out.writeKey(a))
    case Schema.Primitive(StandardType.LongType, _)   => Some((a, out) => out.writeKey(a))
    case Schema.Transform(c, _, g, a, _)              =>
      encodeField(a.foldLeft(c)((s, a) => s.annotate(a))).map { encoder => (b, out) =>
        g(b) match {
          case Left(reason) => throw new RuntimeException(s"Failed to encode key $b: $reason")
          case Right(value) => encoder(value, out)
        }
      }
    case Schema.Lazy(inner)                           => encodeField(inner())
    case _                                            => None
  }

  def decodeField[B](schema: Schema[B]): Option[KeyDecoder[B]] = schema match {
    case Schema.Primitive(StandardType.StringType, _) => Some(_.readKeyAsString())
    case Schema.Primitive(StandardType.CharType, _)   => Some(_.readKeyAsChar())
    case Schema.Primitive(StandardType.BoolType, _)   => Some(_.readKeyAsBoolean())
    case Schema.Primitive(StandardType.ByteType, _)   => Some(_.readKeyAsByte())
    case Schema.Primitive(StandardType.ShortType, _)  => Some(_.readKeyAsShort())
    case Schema.Primitive(StandardType.IntType, _)    => Some(_.readKeyAsInt())
    case Schema.Primitive(StandardType.LongType, _)   => Some(_.readKeyAsLong())
    case Schema.Transform(c, f, _, a, _)              =>
      decodeField(a.foldLeft(c)((s, a) => s.annotate(a))).map { decoder => in =>
        f(decoder(in)) match {
          case Left(reason) => throw new RuntimeException(s"failed to decode key $reason")
          case Right(value) => value
        }
      }
    case Schema.Lazy(inner)                           => decodeField(inner())
    case _                                            => None
  }

  def decodeSeq[Col, Elm](schema: Schema.Sequence[Col, Elm, _]): Decoder[Col] =
    new Decoder[Col] {
      val readChunk = decodeChunk(decodeSchema(schema.elementSchema))

      def apply(in: JsonReader, default: Col): Col =
        schema.fromChunk(readChunk(in, null))
    }

  def decodeNonEmptySeq[Col, Elm](schema: Schema.NonEmptySequence[Col, Elm, _]): Decoder[Col] =
    new Decoder[Col] {
      val readChunk = decodeChunk(decodeSchema(schema.elementSchema))

      def apply(in: JsonReader, default: Col): Col =
        try schema.fromChunk(readChunk(in, null))
        catch { case ex if NonFatal(ex) => in.decodeError(ex.getMessage) }
    }

  def encodeMap[K, V](
    ks: Schema[K],
    vs: Schema[V],
    config: JsoniterCodec.Config,
  ): Encoder[Map[K, V]] = encodeField(ks) match {
    case Some(writeKey) =>
      new Encoder[Map[K, V]] {
        val writeValue: Encoder[V] = encodeSchema(vs, config)

        def apply(a: Map[K, V], out: JsonWriter): Unit = {
          out.writeObjectStart()
          a.foreach { case (key, value) =>
            writeKey(key, out)
            writeValue(value, out)
          }
          out.writeObjectEnd()
        }
      }
    case None           =>
      (a, out) => encodeChunk(encodeTuple(ks, vs, config))(Chunk.fromIterable(a), out)
  }

  def decodeMap[K, V](ks: Schema[K], vs: Schema[V]): Decoder[Map[K, V]] = decodeField(ks) match {
    case Some(readKey) =>
      new Decoder[Map[K, V]] {
        val readValue: Decoder[V] = decodeSchema(vs)

        def apply(in: JsonReader, default: Map[K, V]): Map[K, V] = {
          if (!in.isNextToken('{')) in.decodeError("expected '{'")
          if (in.isNextToken('}')) Map.empty[K, V]
          else {
            in.rollbackToken()
            val kvs = new java.util.LinkedHashMap[K, V](8)
            while ({
              kvs.put(readKey(in), readValue(in, null.asInstanceOf[V]))
              in.isNextToken(',')
            }) ()
            if (!in.isCurrentToken('}')) in.decodeError("expected '}'")
            import scala.jdk.CollectionConverters._
            kvs.asScala.toMap
          }
        }
      }
    case None          =>
      new Decoder[Map[K, V]] {
        val decoder = decodeTuple(ks, vs)

        def apply(in: JsonReader, default: Map[K, V]): Map[K, V] =
          decodeChunk(decoder)(in, null).toMap
      }
  }

  def encodeNonEmptyMap[K, V](ks: Schema[K], vs: Schema[V], config: JsoniterCodec.Config): Encoder[NonEmptyMap[K, V]] =
    new Encoder[NonEmptyMap[K, V]] {
      val writeMap = encodeMap(ks, vs, config)

      def apply(value: NonEmptyMap[K, V], out: JsonWriter): Unit =
        writeMap(value.toMap, out)
    }

  def decodeNonEmptyMap[K, V](ks: Schema[K], vs: Schema[V]): Decoder[NonEmptyMap[K, V]] =
    new Decoder[NonEmptyMap[K, V]] {
      val readMap = decodeMap(ks, vs)

      def apply(in: JsonReader, default: NonEmptyMap[K, V]): NonEmptyMap[K, V] =
        NonEmptyMap.fromMapOption(readMap(in, null)) match {
          case Some(value) => value
          case None        => in.decodeError("NonEmptyMap cannot be empty")
        }
    }

  def decodeSet[A](schema: Schema.Set[A]): Decoder[Set[A]] =
    new Decoder[Set[A]] {
      val readChunk = decodeChunk(decodeSchema(schema.elementSchema))

      def apply(in: JsonReader, default: Set[A]): Set[A] =
        schema.fromChunk(readChunk(in, null))
    }

  def decodeTransform[A, B](schema: Schema.Transform[A, B, _], discriminator: Option[String]): Decoder[B] =
    new Decoder[B] {
      val readValue =
        decodeSchema(schema.annotations.foldLeft(schema.schema)((s, a) => s.annotate(a)), discriminator)

      def apply(in: JsonReader, default: B): B = {
        schema.f(readValue(in, null.asInstanceOf[A])) match {
          case Left(reason) => in.decodeError(reason)
          case Right(value) => value
        }
      }
    }

  def encodeEither[A, B](left: Schema[A], right: Schema[B], config: JsoniterCodec.Config): Encoder[Either[A, B]] =
    new Encoder[Either[A, B]] {
      val writeLeft  = encodeSchema(left, config)
      val writeRight = encodeSchema(right, config)

      def apply(value: Either[A, B], out: JsonWriter): Unit = {
        out.writeObjectStart()
        value match {
          case Left(a)  => out.writeKey("Left"); writeLeft(a, out)
          case Right(a) => out.writeKey("Right"); writeRight(a, out)
        }
        out.writeObjectEnd()
      }
    }

  def decodeEither[A, B](left: Schema[A], right: Schema[B]): Decoder[Either[A, B]] =
    new Decoder[Either[A, B]] {
      val readLeft  = decodeSchema(left)
      val readRight = decodeSchema(right)

      def apply(in: JsonReader, default: Either[A, B]): Either[A, B] = {
        if (!in.isNextToken('{')) in.decodeError("expected '{'")
        val result = in.readKeyAsString() match {
          case "Left"  => Left(readLeft(in, null.asInstanceOf[A]))
          case "Right" => Right(readRight(in, null.asInstanceOf[B]))
        }
        if (!in.isNextToken('}')) in.decodeError("expected '}'")
        result
      }
    }

  def encodeFallback[A, B](left: Schema[A], right: Schema[B], config: JsoniterCodec.Config): Encoder[Fallback[A, B]] =
    new Encoder[Fallback[A, B]] {
      val writeLeftValue  = encodeSchema(left, config)
      val writeRightValue = encodeSchema(right, config)
      val writeBothValues = encodeTuple(left, right, config)

      def apply(value: Fallback[A, B], out: JsonWriter): Unit = value match {
        case Fallback.Left(a)    => writeLeftValue(a, out)
        case Fallback.Right(b)   => writeRightValue(b, out)
        case Fallback.Both(a, b) => writeBothValues((a, b), out)
      }
    }

  def decodeFallback[A, B](left: Schema[A], right: Schema[B], fullDecode: Boolean): Decoder[Fallback[A, B]] =
    new Decoder[Fallback[A, B]] {
      val readLeft  = decodeSchema(left)
      val readRight = decodeSchema(right)

      def apply(in: JsonReader, default: Fallback[A, B]): Fallback[A, B] = {
        var left: Option[A]  = None
        var right: Option[B] = None

        if (in.isNextToken('[')) { // it's both
          try left = Some(readLeft(in, null.asInstanceOf[A]))
          catch {
            case ex if NonFatal(ex) =>
              in.rollbackToken()
              in.skip()
          }
          if (!in.isNextToken(',')) in.commaError()
          if (fullDecode) {
            try right = Some(readRight(in, null.asInstanceOf[B]))
            catch {
              case ex if NonFatal(ex) =>
                in.rollbackToken()
                in.skip()
            }
          }
          if (!in.isNextToken(']')) in.decodeError("expected ']'")
        } else { // it's either left or right
          in.rollbackToken()
          in.setMark()
          try left = Some(readLeft(in, null.asInstanceOf[A]))
          catch {
            case _: Throwable =>
              in.rollbackToMark()
              right = Some(readRight(in, null.asInstanceOf[B]))
          }
        }

        (left, right) match {
          case (Some(a), Some(b)) => Fallback.Both(a, b)
          case (Some(a), _)       => Fallback.Left(a)
          case (_, Some(b))       => Fallback.Right(b)
          case _                  =>
            in.decodeError("fallback decoder was unable to decode both left and right side")
        }
      }
    }

  def encodeRecord[Z](
    schema: Schema.GenericRecord,
    config: JsoniterCodec.Config,
    discriminatorTuple: DiscriminatorTuple,
  ): Encoder[ListMap[String, Any]] = {
    val nonTransientFields = schema.nonTransientFields.toArray

    if (nonTransientFields.isEmpty) {
      new Encoder[ListMap[String, Any]] {
        def apply(value: ListMap[String, Any], out: JsonWriter): Unit = {
          out.writeObjectStart()
          discriminatorTuple.foreach { case (tag, name) =>
            out.writeKey(tag)
            out.writeVal(name)
          }
          out.writeObjectEnd()
        }
      }
    } else {
      new Encoder[ListMap[String, Any]] {

        val fieldEncoders =
          nonTransientFields.map(field => encodeSchema(field.schema.asInstanceOf[Schema[Any]], config))

        def apply(map: ListMap[String, Any], out: JsonWriter): Unit = {
          out.writeObjectStart()
          discriminatorTuple.foreach { case (tag, name) =>
            out.writeKey(tag)
            out.writeVal(name)
          }
          var i = 0
          while (i < nonTransientFields.length) {
            val schema = nonTransientFields(i)
            val value  = map(schema.fieldName)
            if (
              !isEmptyOptionalValue(
                schema,
                value,
                config,
              ) && ((value != null && value != None) || !config.ignoreNullValues)
            ) {
              out.writeKey(schema.fieldName)
              fieldEncoders(i)(value, out)
            }
            i += 1
          }

          out.writeObjectEnd()
        }
      }
    }
  }

  def isEmptyOptionalValue(schema: Schema.Field[_, _], value: Any, config: JsoniterCodec.Config): Boolean = {
    (config.ignoreEmptyCollections || schema.optional) && (value match {
      case None                  => true
      case iterable: Iterable[_] => iterable.isEmpty
      case _                     => false
    })
  }

  def decodeRecord(schema: Schema.GenericRecord, discriminator: Option[String]): Decoder[ListMap[String, Any]] = {
    new Decoder[ListMap[String, Any]] {
      val fields                 = schema.fields.toArray
      val fieldWithReaders       = new java.util.HashMap[String, (String, Decoder[Any])](schema.fields.size << 1)
      schema.fields.foreach { field =>
        val decoder = decodeSchema(field.schema).asInstanceOf[Decoder[Any]]
        field.nameAndAliases.foreach(fieldWithReaders.put(_, (field.fieldName, decoder)))
      }
      val rejectAdditionalFields = schema.annotations.exists(_.isInstanceOf[rejectExtraFields])

      def apply(in: JsonReader, default: ListMap[String, Any]): ListMap[String, Any] = {
        if (!in.isNextToken('{')) in.decodeError("expected '{'")
        var continue = !in.isNextToken('}')
        if (continue) in.rollbackToken()
        val map      = new java.util.HashMap[String, Any](fields.length << 1)
        while (continue) {
          val fieldNameOrAlias = in.readKeyAsString()
          val fieldWithReader  = fieldWithReaders.get(fieldNameOrAlias)
          if (fieldWithReader ne null) {
            val (fieldName, readValue) = fieldWithReader
            val prev                   = map.put(fieldName, readValue(in, null.asInstanceOf[Any]))
            if (prev != null) in.decodeError(s"duplicate field $fieldNameOrAlias")
          } else if (!rejectAdditionalFields || discriminator.contains(fieldNameOrAlias)) {
            in.skip()
          } else in.decodeError(s"extra field $fieldNameOrAlias")
          continue = in.isNextToken(',')
        }
        if (!in.isCurrentToken('}')) in.decodeError("expected '}'")
        var idx      = 0
        while (idx < fields.length) {
          val field     = fields(idx)
          idx += 1
          val fieldName = field.fieldName // reuse strings with calculated hashCode
          if (map.get(fieldName) == null) {
            map.put( // mitigation of a linking error for `map.computeIfAbsent` in Scala.js
              fieldName, {
                if ((field.optional || field.transient) && field.defaultValue.isDefined) {
                  field.defaultValue.get
                } else {
                  var schema = field.schema
                  schema match {
                    case l: Schema.Lazy[_] => schema = l.schema
                    case _                 =>
                  }
                  schema match {
                    case _: Schema.Optional[_]               => None
                    case collection: Schema.Collection[_, _] => collection.empty
                    case _                                   =>
                      in.decodeError(s"missing field ${fieldWithReaders.get(fieldName)._1}")
                  }
                }
              },
            )
          }
        }
        (ListMap.newBuilder[String, Any] ++= ({ // to avoid O(n) insert operations
          import scala.collection.JavaConverters.mapAsScalaMapConverter // use deprecated class for Scala 2.12 compatibility

          map.asScala
        }: @scala.annotation.nowarn)).result()
      }
    }
  }

  // scalafmt: { maxColumn = 400, optIn.configStyleArguments = false }
  def encodeCaseClass[Z](schema: Schema.Record[Z], config: JsoniterCodec.Config, discriminatorTuple: DiscriminatorTuple): Encoder[Z] = new Encoder[Z] {

    val nonTransientFields = schema.nonTransientFields.map(_.asInstanceOf[Schema.Field[Z, Any]]).toArray
    val fieldEncoders      = nonTransientFields.map(field => encodeSchema(field.schema, config))

    def apply(z: Z, out: JsonWriter): Unit = {
      out.writeObjectStart()
      discriminatorTuple.foreach { case (tag, name) =>
        out.writeKey(tag)
        out.writeVal(name)
      }
      var i = 0
      while (i < nonTransientFields.length) {
        val schema = nonTransientFields(i)
        val value  = schema.get(z)
        if (!isEmptyOptionalValue(schema, value, config) && ((value != null && value != None) || !config.ignoreNullValues)) {
          out.writeKey(schema.fieldName)
          fieldEncoders(i)(value, out)
        }
        i += 1
      }
      out.writeObjectEnd()
    }
  }

  def decodeCaseClass0[Z](schema: Schema.CaseClass0[Z]): Decoder[Z] = new Decoder[Z] {

    val rejectExtraFields = schema.annotations.collectFirst { case _: rejectExtraFields => () }.isDefined

    def apply(in: JsonReader, default: Z): Z = {
      if (!in.isNextToken('{')) in.decodeError("expected '{'")
      var continue = !in.isNextToken('}')
      if (continue) in.rollbackToken()
      while (continue) {
        val field = in.readKeyAsString()
        if (rejectExtraFields) in.decodeError(s"extra field $field")
        in.skip()
        continue = in.isNextToken(',')
      }
      if (!in.isCurrentToken('}')) in.decodeError("expected '}'")
      schema.defaultConstruct()
    }
  }

  def decodeCaseClass1[A, Z](schema: Schema.CaseClass1[A, Z], discriminator: Option[String]): Decoder[Z] = { (in: JsonReader, _: Z) =>
    val buffer: Array[Any] = decodeFields(schema, discriminator)(in, Array.empty)
    schema.defaultConstruct(buffer(0).asInstanceOf[A])
  }

  def decodeCaseClass2[A1, A2, Z](schema: Schema.CaseClass2[A1, A2, Z], discriminator: Option[String]): Decoder[Z] = { (in: JsonReader, _: Z) =>
    val buffer: Array[Any] = decodeFields(schema, discriminator)(in, Array.empty)
    schema.construct(buffer(0).asInstanceOf[A1], buffer(1).asInstanceOf[A2])
  }

  def decodeCaseClass3[A1, A2, A3, Z](schema: Schema.CaseClass3[A1, A2, A3, Z], discriminator: Option[String]): Decoder[Z] = { (in: JsonReader, _: Z) =>
    val buffer: Array[Any] = decodeFields(schema, discriminator)(in, Array.empty)
    schema.construct(buffer(0).asInstanceOf[A1], buffer(1).asInstanceOf[A2], buffer(2).asInstanceOf[A3])
  }

  def decodeCaseClass4[A1, A2, A3, A4, Z](schema: Schema.CaseClass4[A1, A2, A3, A4, Z], discriminator: Option[String]): Decoder[Z] = { (in: JsonReader, _: Z) =>
    val buffer: Array[Any] = decodeFields(schema, discriminator)(in, Array.empty)
    schema.construct(buffer(0).asInstanceOf[A1], buffer(1).asInstanceOf[A2], buffer(2).asInstanceOf[A3], buffer(3).asInstanceOf[A4])
  }

  def decodeCaseClass5[A1, A2, A3, A4, A5, Z](schema: Schema.CaseClass5[A1, A2, A3, A4, A5, Z], discriminator: Option[String]): Decoder[Z] = { (in: JsonReader, _: Z) =>
    val buffer: Array[Any] = decodeFields(schema, discriminator)(in, Array.empty)
    schema.construct(buffer(0).asInstanceOf[A1], buffer(1).asInstanceOf[A2], buffer(2).asInstanceOf[A3], buffer(3).asInstanceOf[A4], buffer(4).asInstanceOf[A5])
  }

  def decodeCaseClass6[A1, A2, A3, A4, A5, A6, Z](schema: Schema.CaseClass6[A1, A2, A3, A4, A5, A6, Z], discriminator: Option[String]): Decoder[Z] = { (in: JsonReader, _: Z) =>
    val buffer: Array[Any] = decodeFields(schema, discriminator)(in, Array.empty)
    schema.construct(buffer(0).asInstanceOf[A1], buffer(1).asInstanceOf[A2], buffer(2).asInstanceOf[A3], buffer(3).asInstanceOf[A4], buffer(4).asInstanceOf[A5], buffer(5).asInstanceOf[A6])
  }

  def decodeCaseClass7[A1, A2, A3, A4, A5, A6, A7, Z](schema: Schema.CaseClass7[A1, A2, A3, A4, A5, A6, A7, Z], discriminator: Option[String]): Decoder[Z] = { (in: JsonReader, _: Z) =>
    val buffer: Array[Any] = decodeFields(schema, discriminator)(in, Array.empty)
    schema.construct(buffer(0).asInstanceOf[A1], buffer(1).asInstanceOf[A2], buffer(2).asInstanceOf[A3], buffer(3).asInstanceOf[A4], buffer(4).asInstanceOf[A5], buffer(5).asInstanceOf[A6], buffer(6).asInstanceOf[A7])
  }

  def decodeCaseClass8[A1, A2, A3, A4, A5, A6, A7, A8, Z](schema: Schema.CaseClass8[A1, A2, A3, A4, A5, A6, A7, A8, Z], discriminator: Option[String]): Decoder[Z] = { (in: JsonReader, _: Z) =>
    val buffer: Array[Any] = decodeFields(schema, discriminator)(in, Array.empty)
    schema.construct(buffer(0).asInstanceOf[A1], buffer(1).asInstanceOf[A2], buffer(2).asInstanceOf[A3], buffer(3).asInstanceOf[A4], buffer(4).asInstanceOf[A5], buffer(5).asInstanceOf[A6], buffer(6).asInstanceOf[A7], buffer(7).asInstanceOf[A8])
  }

  def decodeCaseClass9[A1, A2, A3, A4, A5, A6, A7, A8, A9, Z](schema: Schema.CaseClass9[A1, A2, A3, A4, A5, A6, A7, A8, A9, Z], discriminator: Option[String]): Decoder[Z] = { (in: JsonReader, _: Z) =>
    val buffer: Array[Any] = decodeFields(schema, discriminator)(in, Array.empty)
    schema.construct(buffer(0).asInstanceOf[A1], buffer(1).asInstanceOf[A2], buffer(2).asInstanceOf[A3], buffer(3).asInstanceOf[A4], buffer(4).asInstanceOf[A5], buffer(5).asInstanceOf[A6], buffer(6).asInstanceOf[A7], buffer(7).asInstanceOf[A8], buffer(8).asInstanceOf[A9])
  }

  def decodeCaseClass10[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, Z](schema: Schema.CaseClass10[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, Z], discriminator: Option[String]): Decoder[Z] = { (in: JsonReader, _: Z) =>
    val buffer: Array[Any] = decodeFields(schema, discriminator)(in, Array.empty)
    schema.construct(buffer(0).asInstanceOf[A1], buffer(1).asInstanceOf[A2], buffer(2).asInstanceOf[A3], buffer(3).asInstanceOf[A4], buffer(4).asInstanceOf[A5], buffer(5).asInstanceOf[A6], buffer(6).asInstanceOf[A7], buffer(7).asInstanceOf[A8], buffer(8).asInstanceOf[A9], buffer(9).asInstanceOf[A10])
  }

  def decodeCaseClass11[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, Z](schema: Schema.CaseClass11[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, Z], discriminator: Option[String]): Decoder[Z] = { (in: JsonReader, _: Z) =>
    val buffer: Array[Any] = decodeFields(schema, discriminator)(in, Array.empty)
    schema.construct(buffer(0).asInstanceOf[A1], buffer(1).asInstanceOf[A2], buffer(2).asInstanceOf[A3], buffer(3).asInstanceOf[A4], buffer(4).asInstanceOf[A5], buffer(5).asInstanceOf[A6], buffer(6).asInstanceOf[A7], buffer(7).asInstanceOf[A8], buffer(8).asInstanceOf[A9], buffer(9).asInstanceOf[A10], buffer(10).asInstanceOf[A11])
  }

  def decodeCaseClass12[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, Z](schema: Schema.CaseClass12[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, Z], discriminator: Option[String]): Decoder[Z] = { (in: JsonReader, _: Z) =>
    val buffer: Array[Any] = decodeFields(schema, discriminator)(in, Array.empty)
    schema.construct(buffer(0).asInstanceOf[A1], buffer(1).asInstanceOf[A2], buffer(2).asInstanceOf[A3], buffer(3).asInstanceOf[A4], buffer(4).asInstanceOf[A5], buffer(5).asInstanceOf[A6], buffer(6).asInstanceOf[A7], buffer(7).asInstanceOf[A8], buffer(8).asInstanceOf[A9], buffer(9).asInstanceOf[A10], buffer(10).asInstanceOf[A11], buffer(11).asInstanceOf[A12])
  }

  def decodeCaseClass13[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, Z](schema: Schema.CaseClass13[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, Z], discriminator: Option[String]): Decoder[Z] = { (in: JsonReader, _: Z) =>
    val buffer: Array[Any] = decodeFields(schema, discriminator)(in, Array.empty)
    schema.construct(buffer(0).asInstanceOf[A1], buffer(1).asInstanceOf[A2], buffer(2).asInstanceOf[A3], buffer(3).asInstanceOf[A4], buffer(4).asInstanceOf[A5], buffer(5).asInstanceOf[A6], buffer(6).asInstanceOf[A7], buffer(7).asInstanceOf[A8], buffer(8).asInstanceOf[A9], buffer(9).asInstanceOf[A10], buffer(10).asInstanceOf[A11], buffer(11).asInstanceOf[A12], buffer(12).asInstanceOf[A13])
  }

  def decodeCaseClass14[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, Z](schema: Schema.CaseClass14[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, Z], discriminator: Option[String]): Decoder[Z] = { (in: JsonReader, _: Z) =>
    val buffer: Array[Any] = decodeFields(schema, discriminator)(in, Array.empty)
    schema.construct(
      buffer(0).asInstanceOf[A1],
      buffer(1).asInstanceOf[A2],
      buffer(2).asInstanceOf[A3],
      buffer(3).asInstanceOf[A4],
      buffer(4).asInstanceOf[A5],
      buffer(5).asInstanceOf[A6],
      buffer(6).asInstanceOf[A7],
      buffer(7).asInstanceOf[A8],
      buffer(8).asInstanceOf[A9],
      buffer(9).asInstanceOf[A10],
      buffer(10).asInstanceOf[A11],
      buffer(11).asInstanceOf[A12],
      buffer(12).asInstanceOf[A13],
      buffer(13).asInstanceOf[A14],
    )
  }

  def decodeCaseClass15[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, Z](schema: Schema.CaseClass15[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, Z], discriminator: Option[String]): Decoder[Z] = { (in: JsonReader, _: Z) =>
    val buffer: Array[Any] = decodeFields(schema, discriminator)(in, Array.empty)
    schema.construct(
      buffer(0).asInstanceOf[A1],
      buffer(1).asInstanceOf[A2],
      buffer(2).asInstanceOf[A3],
      buffer(3).asInstanceOf[A4],
      buffer(4).asInstanceOf[A5],
      buffer(5).asInstanceOf[A6],
      buffer(6).asInstanceOf[A7],
      buffer(7).asInstanceOf[A8],
      buffer(8).asInstanceOf[A9],
      buffer(9).asInstanceOf[A10],
      buffer(10).asInstanceOf[A11],
      buffer(11).asInstanceOf[A12],
      buffer(12).asInstanceOf[A13],
      buffer(13).asInstanceOf[A14],
      buffer(14).asInstanceOf[A15],
    )
  }

  def decodeCaseClass16[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, Z](schema: Schema.CaseClass16[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, Z], discriminator: Option[String]): Decoder[Z] = { (in: JsonReader, _: Z) =>
    val buffer: Array[Any] = decodeFields(schema, discriminator)(in, Array.empty)
    schema.construct(
      buffer(0).asInstanceOf[A1],
      buffer(1).asInstanceOf[A2],
      buffer(2).asInstanceOf[A3],
      buffer(3).asInstanceOf[A4],
      buffer(4).asInstanceOf[A5],
      buffer(5).asInstanceOf[A6],
      buffer(6).asInstanceOf[A7],
      buffer(7).asInstanceOf[A8],
      buffer(8).asInstanceOf[A9],
      buffer(9).asInstanceOf[A10],
      buffer(10).asInstanceOf[A11],
      buffer(11).asInstanceOf[A12],
      buffer(12).asInstanceOf[A13],
      buffer(13).asInstanceOf[A14],
      buffer(14).asInstanceOf[A15],
      buffer(15).asInstanceOf[A16],
    )
  }

  def decodeCaseClass17[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, Z](schema: Schema.CaseClass17[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, Z], discriminator: Option[String]): Decoder[Z] = { (in: JsonReader, _: Z) =>
    val buffer: Array[Any] = decodeFields(schema, discriminator)(in, Array.empty)
    schema.construct(
      buffer(0).asInstanceOf[A1],
      buffer(1).asInstanceOf[A2],
      buffer(2).asInstanceOf[A3],
      buffer(3).asInstanceOf[A4],
      buffer(4).asInstanceOf[A5],
      buffer(5).asInstanceOf[A6],
      buffer(6).asInstanceOf[A7],
      buffer(7).asInstanceOf[A8],
      buffer(8).asInstanceOf[A9],
      buffer(9).asInstanceOf[A10],
      buffer(10).asInstanceOf[A11],
      buffer(11).asInstanceOf[A12],
      buffer(12).asInstanceOf[A13],
      buffer(13).asInstanceOf[A14],
      buffer(14).asInstanceOf[A15],
      buffer(15).asInstanceOf[A16],
      buffer(16).asInstanceOf[A17],
    )
  }

  def decodeCaseClass18[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, Z](schema: Schema.CaseClass18[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, Z], discriminator: Option[String]): Decoder[Z] = { (in: JsonReader, _: Z) =>
    val buffer: Array[Any] = decodeFields(schema, discriminator)(in, Array.empty)
    schema.construct(
      buffer(0).asInstanceOf[A1],
      buffer(1).asInstanceOf[A2],
      buffer(2).asInstanceOf[A3],
      buffer(3).asInstanceOf[A4],
      buffer(4).asInstanceOf[A5],
      buffer(5).asInstanceOf[A6],
      buffer(6).asInstanceOf[A7],
      buffer(7).asInstanceOf[A8],
      buffer(8).asInstanceOf[A9],
      buffer(9).asInstanceOf[A10],
      buffer(10).asInstanceOf[A11],
      buffer(11).asInstanceOf[A12],
      buffer(12).asInstanceOf[A13],
      buffer(13).asInstanceOf[A14],
      buffer(14).asInstanceOf[A15],
      buffer(15).asInstanceOf[A16],
      buffer(16).asInstanceOf[A17],
      buffer(17).asInstanceOf[A18],
    )
  }

  def decodeCaseClass19[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, Z](schema: Schema.CaseClass19[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, Z], discriminator: Option[String]): Decoder[Z] = { (in: JsonReader, _: Z) =>
    val buffer: Array[Any] = decodeFields(schema, discriminator)(in, Array.empty)
    schema.construct(
      buffer(0).asInstanceOf[A1],
      buffer(1).asInstanceOf[A2],
      buffer(2).asInstanceOf[A3],
      buffer(3).asInstanceOf[A4],
      buffer(4).asInstanceOf[A5],
      buffer(5).asInstanceOf[A6],
      buffer(6).asInstanceOf[A7],
      buffer(7).asInstanceOf[A8],
      buffer(8).asInstanceOf[A9],
      buffer(9).asInstanceOf[A10],
      buffer(10).asInstanceOf[A11],
      buffer(11).asInstanceOf[A12],
      buffer(12).asInstanceOf[A13],
      buffer(13).asInstanceOf[A14],
      buffer(14).asInstanceOf[A15],
      buffer(15).asInstanceOf[A16],
      buffer(16).asInstanceOf[A17],
      buffer(17).asInstanceOf[A18],
      buffer(18).asInstanceOf[A19],
    )
  }

  def decodeCaseClass20[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, Z](schema: Schema.CaseClass20[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, Z], discriminator: Option[String]): Decoder[Z] = { (in: JsonReader, _: Z) =>
    val buffer: Array[Any] = decodeFields(schema, discriminator)(in, Array.empty)
    schema.construct(
      buffer(0).asInstanceOf[A1],
      buffer(1).asInstanceOf[A2],
      buffer(2).asInstanceOf[A3],
      buffer(3).asInstanceOf[A4],
      buffer(4).asInstanceOf[A5],
      buffer(5).asInstanceOf[A6],
      buffer(6).asInstanceOf[A7],
      buffer(7).asInstanceOf[A8],
      buffer(8).asInstanceOf[A9],
      buffer(9).asInstanceOf[A10],
      buffer(10).asInstanceOf[A11],
      buffer(11).asInstanceOf[A12],
      buffer(12).asInstanceOf[A13],
      buffer(13).asInstanceOf[A14],
      buffer(14).asInstanceOf[A15],
      buffer(15).asInstanceOf[A16],
      buffer(16).asInstanceOf[A17],
      buffer(17).asInstanceOf[A18],
      buffer(18).asInstanceOf[A19],
      buffer(19).asInstanceOf[A20],
    )
  }

  def decodeCaseClass21[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, A21, Z](schema: Schema.CaseClass21[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, A21, Z], discriminator: Option[String]): Decoder[Z] = { (in: JsonReader, _: Z) =>
    val buffer: Array[Any] = decodeFields(schema, discriminator)(in, Array.empty)
    schema.construct(
      buffer(0).asInstanceOf[A1],
      buffer(1).asInstanceOf[A2],
      buffer(2).asInstanceOf[A3],
      buffer(3).asInstanceOf[A4],
      buffer(4).asInstanceOf[A5],
      buffer(5).asInstanceOf[A6],
      buffer(6).asInstanceOf[A7],
      buffer(7).asInstanceOf[A8],
      buffer(8).asInstanceOf[A9],
      buffer(9).asInstanceOf[A10],
      buffer(10).asInstanceOf[A11],
      buffer(11).asInstanceOf[A12],
      buffer(12).asInstanceOf[A13],
      buffer(13).asInstanceOf[A14],
      buffer(14).asInstanceOf[A15],
      buffer(15).asInstanceOf[A16],
      buffer(16).asInstanceOf[A17],
      buffer(17).asInstanceOf[A18],
      buffer(18).asInstanceOf[A19],
      buffer(19).asInstanceOf[A20],
      buffer(20).asInstanceOf[A21],
    )
  }

  def decodeCaseClass22[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, A21, A22, Z](schema: Schema.CaseClass22[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, A21, A22, Z], discriminator: Option[String]): Decoder[Z] = { (in: JsonReader, _: Z) =>
    val buffer: Array[Any] = decodeFields(schema, discriminator)(in, Array.empty)
    schema.construct(
      buffer(0).asInstanceOf[A1],
      buffer(1).asInstanceOf[A2],
      buffer(2).asInstanceOf[A3],
      buffer(3).asInstanceOf[A4],
      buffer(4).asInstanceOf[A5],
      buffer(5).asInstanceOf[A6],
      buffer(6).asInstanceOf[A7],
      buffer(7).asInstanceOf[A8],
      buffer(8).asInstanceOf[A9],
      buffer(9).asInstanceOf[A10],
      buffer(10).asInstanceOf[A11],
      buffer(11).asInstanceOf[A12],
      buffer(12).asInstanceOf[A13],
      buffer(13).asInstanceOf[A14],
      buffer(14).asInstanceOf[A15],
      buffer(15).asInstanceOf[A16],
      buffer(16).asInstanceOf[A17],
      buffer(17).asInstanceOf[A18],
      buffer(18).asInstanceOf[A19],
      buffer(19).asInstanceOf[A20],
      buffer(20).asInstanceOf[A21],
      buffer(21).asInstanceOf[A22],
    )
  }

  private def decodeFields[Z](schema: Schema.Record[Z], discriminator: Option[String]): Decoder[Array[Any]] = new Decoder[Array[Any]] {

    val len      = schema.fields.length
    val fields   = new Array[Schema.Field[Z, _]](len)
    val decoders = new Array[Decoder[Any]](len)
    val names    = Array.newBuilder[String]
    val aliases  = new java.util.HashMap[String, Int](len << 1)

    var i = 0
    schema.fields.foreach { field =>
      fields(i) = field
      decoders(i) = decodeSchema(field.schema).asInstanceOf[Decoder[Any]]
      val name = field.fieldName
      names += name
      aliases.put(name, i)
      field.annotations.foreach {
        case fna: fieldNameAliases => fna.aliases.foreach(a => aliases.put(a, i))
        case _                     =>
      }
      i += 1
    }

    discriminator.foreach { name =>
      names += name
      aliases.put(name, len)
    }

    val rejectExtraFields = schema.annotations.exists(_.isInstanceOf[rejectExtraFields])

    def apply(in: JsonReader, default: Array[Any]): Array[Any] = {
      if (!in.isNextToken('{')) in.decodeError("expected '{'")
      var continue = !in.isNextToken('}')
      if (continue) in.rollbackToken()
      val len      = fields.length
      val buffer   = new Array[Any](len)
      while (continue) {
        val key = in.readKeyAsString()
        val idx = aliases.getOrDefault(key, -1)
        if (idx >= 0) {
          if (idx == len) in.skip() // skipping discriminator field values
          else if (buffer(idx) == null) buffer(idx) = decoders(idx)(in, null.asInstanceOf[Any])
          else in.decodeError(s"duplicate field $key")
        } else if (!rejectExtraFields) in.skip()
        else in.decodeError(s"extra field $key")
        continue = in.isNextToken(',')
      }
      if (!in.isCurrentToken('}')) in.decodeError("expected '}'")
      var idx      = 0
      while (idx < len) {
        if (buffer(idx) == null) {
          val field = fields(idx)
          if ((field.optional || field.transient) && field.defaultValue.isDefined) {
            buffer(idx) = field.defaultValue.get
          } else {
            var schema = field.schema
            schema match {
              case l: Schema.Lazy[_] => schema = l.schema
              case _                 =>
            }
            buffer(idx) = schema match {
              case _: Schema.Optional[_]               => None
              case collection: Schema.Collection[_, _] => collection.empty
              case _                                   => in.decodeError(s"missing field ${field.fieldName}")
            }
          }
        }
        idx += 1
      }
      buffer
    }
  }

  def encodeEnum[Z](schema: Schema.Enum[Z], config: JsoniterCodec.Config): Encoder[Z] = {
    // if all cases are CaseClass0, encode as a String
    if (schema.annotations.exists(_.isInstanceOf[simpleEnum])) {
      new Encoder[Z] {
        val cases = new java.util.HashMap[Z, String](schema.cases.size << 1)
        schema.nonTransientCases.foreach { _case =>
          cases.put(_case.schema.asInstanceOf[Schema.CaseClass0[Z]].defaultConstruct(), _case.caseName)
        }

        def apply(value: Z, out: JsonWriter): Unit = {
          val caseName = cases.getOrDefault(value, null)
          if (caseName eq null) out.encodeError(s"encoding non-matching enum case $value")
          out.writeVal(caseName)
        }
      }
    } else {
      val discriminatorName    =
        if (schema.noDiscriminator) None
        else schema.annotations.collectFirst { case d: discriminatorName => d.tag }
      val doJsonObjectWrapping = discriminatorName.isEmpty && !schema.noDiscriminator

      if (doJsonObjectWrapping) {
        new Encoder[Z] {
          val cases    = schema.nonTransientCases.toArray
          val encoders = cases.map { case_ =>
            val discriminatorTuple = discriminatorName.map(_ -> case_.caseName)
            encodeSchema(case_.schema.asInstanceOf[Schema[Any]], config, discriminatorTuple)
          }
          val keys     = cases.map(_.caseName) // TODO: try to pre-encode keys and values?

          def apply(value: Z, out: JsonWriter): Unit = {
            var idx = 0
            while (idx < cases.length) {
              val case_ = cases(idx)
              if (case_.isCase(value)) {
                out.writeObjectStart()
                out.writeKey(keys(idx))
                encoders(idx)(value, out)
                out.writeObjectEnd()
                return
              }
              idx += 1
            }
            out.writeObjectStart() // for transient cases
            out.writeObjectEnd()
          }
        }
      } else {
        new Encoder[Z] {
          val cases    = schema.nonTransientCases.toArray
          val encoders = cases.map { case_ =>
            val discriminatorTuple = discriminatorName.map(_ -> case_.caseName)
            encodeSchema(case_.schema.asInstanceOf[Schema[Any]], config, discriminatorTuple)
          }

          def apply(value: Z, out: JsonWriter): Unit = {
            var idx = 0
            while (idx < cases.length) {
              val case_ = cases(idx)
              if (case_.isCase(value)) {
                encoders(idx)(value, out)
                return
              }
              idx += 1
            }
            out.writeObjectStart() // for transient cases
            out.writeObjectEnd()
          }
        }
      }
    }
  }

  def decodeEnum[Z](parentSchema: Schema.Enum[Z]): Decoder[Z] = {
    val caseNameAliases = new mutable.HashMap[String, Schema.Case[Z, Any]]
    parentSchema.cases.foreach { case_ =>
      val schema = case_.asInstanceOf[Schema.Case[Z, Any]]
      caseNameAliases.put(schema.caseName, schema)
      schema.caseNameAliases.foreach { alias => caseNameAliases.put(alias, schema) }
    }

    if (parentSchema.cases.forall(_.schema.isInstanceOf[Schema.CaseClass0[_]])) { // if all cases are CaseClass0, decode as String
      new Decoder[Z] {
        val cases = new java.util.HashMap[String, Z](caseNameAliases.size << 1)
        caseNameAliases.foreach { case (name, case_) =>
          cases.put(name, case_.schema.asInstanceOf[Schema.CaseClass0[Z]].defaultConstruct())
        }

        def apply(in: JsonReader, default: Z): Z = {
          val subtype = in.readString(null)
          val result  = cases.get(subtype)
          if (result == null) in.decodeError(s"unrecognized subtype $subtype")
          result
        }
      }
    } else if (parentSchema.annotations.exists(_.isInstanceOf[noDiscriminator])) {
      new Decoder[Z] {
        val decoders = parentSchema.cases.toArray.map(c => decodeSchema(c.schema).asInstanceOf[Decoder[Any]])

        def apply(in: JsonReader, default: Z): Z = {
          val it = decoders.iterator
          while (it.hasNext) {
            in.setMark()
            try return it.next()(in, null).asInstanceOf[Z]
            catch { case ex if NonFatal(ex) => in.rollbackToMark() }
          }
          in.decodeError("none of the subtypes could decode the data")
        }
      }
    } else {
      val discriminator = parentSchema.annotations.collectFirst { case d: discriminatorName => d.tag }
      discriminator match {
        case None       =>
          new Decoder[Z] {
            val decoders = new java.util.HashMap[String, Decoder[Any]](caseNameAliases.size << 1)
            caseNameAliases.foreach { case (name, case_) =>
              decoders.put(name, decodeSchema(case_.schema, discriminator))
            }

            def apply(in: JsonReader, default: Z): Z = {
              if (in.nextToken() != '{') in.decodeError("expected '{'")
              if (in.isNextToken('}')) in.decodeError("missing subtype")
              in.rollbackToken()
              val key     = in.readKeyAsString()
              val decoder = decoders.getOrDefault(key, null)
              if (decoder eq null) in.decodeError(s"unrecognized subtype $key")
              val decoded = decoder(in, null).asInstanceOf[Z]
              if (in.nextToken() != '}') in.decodeError("expected '}'")
              decoded
            }
          }
        case Some(name) =>
          new Decoder[Z] {
            val decoders = new java.util.HashMap[String, Decoder[Any]](caseNameAliases.size << 1)
            caseNameAliases.foreach { case (name, case_) =>
              decoders.put(name, decodeSchema(case_.schema, discriminator))
            }

            def apply(in: JsonReader, default: Z): Z = {
              in.setMark()
              if (in.nextToken() != '{') in.decodeError("expected '{'")
              if (in.nextToken() == '}') in.decodeError(s"missing subtype $name")
              in.rollbackToken()
              while ({
                if (in.readKeyAsString() != name) {
                  in.skip()
                  in.nextToken() match {
                    case '}' => in.decodeError(s"missing subtype $name")
                    case ',' => true
                    case _   => in.commaError()
                  }
                } else false
              }) ()
              val subtype = in.readString(null)
              in.rollbackToMark()
              val decoder = decoders.get(subtype)
              if (decoder eq null) in.decodeError(s"unrecognized subtype $subtype")
              decoder(in, null).asInstanceOf[Z]
            }
          }
      }
    }
  }

  def encodeDynamic(schema: Schema.Dynamic, config: JsoniterCodec.Config): Encoder[DynamicValue] = {
    if (schema.annotations.exists(_.isInstanceOf[directDynamicMapping])) {
      new Encoder[DynamicValue] { directEncoder =>
        def apply(value: DynamicValue, out: JsonWriter): Unit = value match {
          case DynamicValue.Record(_, values)              =>
            out.writeObjectStart()
            values.foreach { case (key, value) =>
              out.writeKey(key)
              directEncoder(value, out)
            }
            out.writeObjectEnd()
          case DynamicValue.Enumeration(_, _)              =>
            throw new Exception(s"DynamicValue.Enumeration is not supported in directDynamicMapping mode")
          case DynamicValue.Sequence(values)               => encodeChunk(directEncoder)(values, out)
          case DynamicValue.Dictionary(_)                  =>
            throw new Exception(s"DynamicValue.Dictionary is not supported in directDynamicMapping mode")
          case DynamicValue.SetValue(values)               => encodeChunk(directEncoder)(Chunk.fromIterable(values), out)
          case DynamicValue.Primitive(value, standardType) => encodePrimitive(standardType)(value, out)
          case DynamicValue.Singleton(_)                   =>
            out.writeObjectStart()
            out.writeObjectEnd()
          case DynamicValue.SomeValue(value)               => directEncoder(value, out)
          case DynamicValue.NoneValue                      => out.writeNull()
          case DynamicValue.Tuple(_, _)                    =>
            throw new Exception(s"DynamicValue.Tuple is not supported in directDynamicMapping mode")
          case DynamicValue.LeftValue(_)                   =>
            throw new Exception(s"DynamicValue.LeftValue is not supported in directDynamicMapping mode")
          case DynamicValue.RightValue(_)                  =>
            throw new Exception(s"DynamicValue.RightValue is not supported in directDynamicMapping mode")
          case DynamicValue.BothValue(left, right)         =>
            out.writeArrayStart()
            directEncoder(left, out)
            directEncoder(right, out)
            out.writeArrayEnd()
          case DynamicValue.DynamicAst(_)                  =>
            throw new Exception(s"DynamicValue.DynamicAst is not supported in directDynamicMapping mode")
          case DynamicValue.Error(message)                 =>
            throw new Exception(message)
        }
      }
    } else {
      encodeSchema(DynamicValue.schema, config)
    }
  }

  private def decodeDynamic(schema: Schema.Dynamic): Decoder[DynamicValue] = {
    if (schema.annotations.exists(_.isInstanceOf[directDynamicMapping])) {
      new Decoder[DynamicValue] { directDecoder =>
        def apply(in: JsonReader, default: DynamicValue): DynamicValue = {
          val b = in.nextToken()
          if (b == '"') {
            in.rollbackToken()
            DynamicValue.Primitive(in.readString(null), StandardType.StringType)
          } else if (b == 'f' || b == 't') {
            in.rollbackToken()
            DynamicValue.Primitive(in.readBoolean(), StandardType.BoolType)
          } else if ((b >= '0' && b <= '9') || b == '-') {
            in.rollbackToken()
            DynamicValue.Primitive(in.readBigDecimal(null, JsonReader.bigDecimalMathContext, JsonReader.bigDecimalScaleLimit, JsonReader.bigDecimalDigitsLimit).underlying, StandardType.BigDecimalType)
          } else if (b == '[') {
            if (in.isNextToken(']')) DynamicValue.Sequence(Chunk.empty)
            else {
              in.rollbackToken()
              var vs = new Array[DynamicValue](8)
              var i  = 0
              while ({
                if (i == vs.length) vs = java.util.Arrays.copyOf(vs, i << 1)
                vs(i) = directDecoder(in, null)
                i += 1
                in.isNextToken(',')
              }) ()
              if (in.isCurrentToken(']')) {
                DynamicValue.Sequence(Chunk.fromArray {
                  if (i == vs.length) vs
                  else java.util.Arrays.copyOf(vs, i)
                })
              } else in.arrayEndOrCommaError()
            }
          } else if (b == '{') {
            if (in.isNextToken('}')) DynamicValue.Record(TypeId.Structural, ListMap.empty)
            else {
              in.rollbackToken()
              val kvs = new java.util.LinkedHashMap[String, DynamicValue](8)
              while ({
                kvs.put(in.readKeyAsString(), directDecoder(in, null))
                in.isNextToken(',')
              }) ()
              if (in.isCurrentToken('}')) {
                DynamicValue.Record(
                  TypeId.Structural, {
                    import scala.jdk.CollectionConverters._
                    ListMap(kvs.asScala.toSeq: _*)
                  },
                )
              } else in.objectEndOrCommaError()
            }
          } else {
            in.readNullOrError(DynamicValue.NoneValue, "expected value")
          }
        }
      }
    } else {
      decodeSchema(DynamicValue.schema)
    }
  }
}
