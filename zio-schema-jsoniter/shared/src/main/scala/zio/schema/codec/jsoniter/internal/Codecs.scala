package zio.schema.codec.jsoniter.internal

import com.github.plokhotnyuk.jsoniter_scala.core._
import zio.prelude.NonEmptyMap
import zio.schema._
import zio.schema.annotation._
import zio.schema.codec.jsoniter.JsoniterCodec
import zio.{Chunk, ChunkBuilder, Unsafe}

import java.util.concurrent.ConcurrentHashMap
import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.util.control.NonFatal

private[jsoniter] object Codecs extends Codecs

private[jsoniter] trait Codecs {

  private case class CodecKey[A](
    schema: Schema[A],
    config: JsoniterCodec.Configuration,
    discriminator: Option[String],
  ) {
    override val hashCode: Int             =
      System.identityHashCode(schema) ^ config.hashCode ^ discriminator.hashCode
    override def equals(obj: Any): Boolean = obj match {
      case x: CodecKey[_] => (x.schema eq schema) && x.config == config && x.discriminator == discriminator
      case _              => false
    }
  }

  private val codecs = new ConcurrentHashMap[CodecKey[_], JsonValueCodec[_]]()

  def schemaCodec[A](
    schema: Schema[A],
    config: JsoniterCodec.Configuration,
    discriminator: Option[String] = None,
  ): JsonValueCodec[A] = {
    val key                      = CodecKey(schema, config, discriminator)
    var codec: JsonValueCodec[A] = codecs.get(key).asInstanceOf[JsonValueCodec[A]]
    if (codec eq null) {
      codec = schemaCodecSlow(schema, config, discriminator)
      codecs.put(key, codec)
    }
    codec
  }

  def schemaCodecSlow[A](
    schema: Schema[A],
    config: JsoniterCodec.Configuration,
    discriminator: Option[String] = None,
  ): JsonValueCodec[A] = schema match {
    case Schema.Primitive(standardType, _)           => primitiveCodec(standardType)
    case Schema.Optional(schema, _)                  => optionCodec(schema, config)
    case Schema.Tuple2(l, r, _)                      => tupleCodec(l, r, config)
    case Schema.Sequence(schema, f, g, _, _)         =>
      new JsonValueCodec[A] {
        val codec = chunkCodec(schemaCodec(schema, config))

        override def decodeValue(in: JsonReader, default: A): A   =
          f(codec.decodeValue(in, null))
        override def encodeValue(value: A, out: JsonWriter): Unit =
          codec.encodeValue(g(value), out)
        override def nullValue: A                                 = null.asInstanceOf[A]
      }
    case Schema.NonEmptySequence(schema, f, g, _, _) =>
      new JsonValueCodec[A] {
        val codec = chunkCodec(schemaCodec(schema, config))

        override def decodeValue(in: JsonReader, default: A): A   =
          try f(codec.decodeValue(in, null)).get
          catch { case ex if NonFatal(ex) => in.decodeError(ex.getMessage) }
        override def encodeValue(value: A, out: JsonWriter): Unit =
          codec.encodeValue(g(value), out)
        override def nullValue: A                                 = null.asInstanceOf[A]
      }
    case Schema.Map(ks, vs, _)                       => mapCodec(ks, vs, config)
    case Schema.NonEmptyMap(ks, vs, _)               => nonEmptyMapCodec(ks, vs, config)
    case s @ Schema.Set(schema, _)                   =>
      new JsonValueCodec[A] {
        val codec = chunkCodec(schemaCodec(schema, config))

        override def decodeValue(in: JsonReader, default: A): A   =
          s.fromChunk(codec.decodeValue(in, null))
        override def encodeValue(value: A, out: JsonWriter): Unit =
          codec.encodeValue(s.toChunk(value), out)
        override def nullValue: A                                 = null.asInstanceOf[A]
      }
    case Schema.Transform(c, f, g, a, _)             => transformCodec(c, f, g, a, config, discriminator)
    case Schema.Fail(message, _)                     =>
      new JsonValueCodec[A] {
        override def decodeValue(in: JsonReader, default: A): A =
          in.decodeError(message)
        override def encodeValue(x: A, out: JsonWriter): Unit   = {
          out.writeObjectStart()
          out.writeObjectEnd()
        }
        override def nullValue: A                               = null.asInstanceOf[A]
      }
    case Schema.Either(left, right, _)               => eitherCodec(left, right, config)
    case Schema.Fallback(left, right, fullDecode, _) => fallbackCodec(left, right, config, fullDecode)
    case s: Schema.Lazy[A]                           => lazyCodec(s, config, discriminator)
    case s: Schema.GenericRecord                     => recordCodec(s, config, discriminator)
    case s: Schema.Record[A]                         => caseClassCodec(s, config, discriminator)
    case s: Schema.Enum[A]                           => enumCodec(s, config)
    case s: Schema.Dynamic                           => dynamicCodec(s, config)
    case null                                        =>
      throw new Exception(s"A captured schema is null, most likely due to wrong field initialization order")
  }

  def chunkCodec[A](implicit codec: JsonValueCodec[A]): JsonValueCodec[Chunk[A]] = new JsonValueCodec[Chunk[A]] {

    override def decodeValue(in: JsonReader, default: Chunk[A]): Chunk[A] = {
      if (!in.isNextToken('[')) in.decodeError("expected '['")
      if (in.isNextToken(']')) Chunk.empty[A]
      else {
        in.rollbackToken()
        val builder = ChunkBuilder.make[A](8)
        while ({
          builder += codec.decodeValue(in, null.asInstanceOf[A])
          in.isNextToken(',')
        }) ()
        if (in.isCurrentToken(']')) builder.result()
        else in.arrayEndOrCommaError()
      }
    }

    override def encodeValue(xs: Chunk[A], out: JsonWriter): Unit = {
      out.writeArrayStart()
      val it: Iterator[A] = xs.iterator
      while (it.hasNext) codec.encodeValue(it.next(), out)
      out.writeArrayEnd()
    }

    override def nullValue: Chunk[A] = null.asInstanceOf[Chunk[A]]
  }

  @inline
  private def primitive[A](decode: JsonReader => A, encode: (A, JsonWriter) => Unit): JsonValueCodec[A] =
    new JsonValueCodec[A] {
      @inline
      override def decodeValue(in: JsonReader, default: A): A = decode(in)
      @inline
      override def encodeValue(x: A, out: JsonWriter): Unit   = encode(x, out)
      override def nullValue: A                               = null.asInstanceOf[A]
    }

  def primitiveCodec[A](standardType: StandardType[A]): JsonValueCodec[A] = standardType match {
    case StandardType.UnitType           =>
      new JsonValueCodec[A] {
        override def decodeValue(in: JsonReader, default: A): A = {
          in.nextToken() match {
            case '{' => if (in.isNextToken('}')) () else in.decodeError("expected '}'")
            case '[' => if (in.isNextToken(']')) () else in.decodeError("expected ']'")
            case _   => in.readNullOrError((), "expected '{' or '[' or null")
          }
        }
        override def encodeValue(x: A, out: JsonWriter): Unit   = {
          out.writeObjectStart()
          out.writeObjectEnd()
        }
        override def nullValue: A                               = null.asInstanceOf[A]
      }
    case StandardType.StringType         => primitive(_.readString(null), (a, out) => out.writeVal(a))
    case StandardType.BoolType           => primitive(_.readBoolean(), (a, out) => out.writeVal(a))
    case StandardType.ByteType           => primitive(_.readByte(), (a, out) => out.writeVal(a))
    case StandardType.ShortType          => primitive(_.readShort(), (a, out) => out.writeVal(a))
    case StandardType.IntType            => primitive(_.readInt(), (a, out) => out.writeVal(a))
    case StandardType.LongType           => primitive(_.readLong(), (a, out) => out.writeVal(a))
    case StandardType.FloatType          => primitive(_.readFloat(), (a, out) => out.writeVal(a))
    case StandardType.DoubleType         => primitive(_.readDouble(), (a, out) => out.writeVal(a))
    case StandardType.BinaryType         =>
      new JsonValueCodec[A] {
        val codec = chunkCodec[Byte](primitive(_.readByte(), (a, out) => out.writeVal(a)))

        override def decodeValue(in: JsonReader, default: A): A = codec.decodeValue(in, default)
        override def encodeValue(x: A, out: JsonWriter): Unit   = codec.encodeValue(x, out)
        override def nullValue: A                               = null.asInstanceOf[A]
      }
    case StandardType.CharType           => primitive(_.readChar(), (a, out) => out.writeVal(a))
    case StandardType.BigIntegerType     =>
      primitive(_.readBigInt(null).underlying, (a, out) => out.writeVal(BigInt(a)))
    case StandardType.BigDecimalType     =>
      primitive(_.readBigDecimal(null).underlying, (a, out) => out.writeVal(BigDecimal(a)))
    case StandardType.UUIDType           => primitive(_.readUUID(null), (a, out) => out.writeVal(a))
    case StandardType.DayOfWeekType      =>
      primitive(in => java.time.DayOfWeek.valueOf(in.readString(null)), (a, out) => out.writeVal(a.toString))
    case StandardType.DurationType       => primitive(_.readDuration(null), (a, out) => out.writeVal(a))
    case StandardType.InstantType        => primitive(_.readInstant(null), (a, out) => out.writeVal(a))
    case StandardType.LocalDateType      => primitive(_.readLocalDate(null), (a, out) => out.writeVal(a))
    case StandardType.LocalDateTimeType  => primitive(_.readLocalDateTime(null), (a, out) => out.writeVal(a))
    case StandardType.LocalTimeType      => primitive(_.readLocalTime(null), (a, out) => out.writeVal(a))
    case StandardType.MonthType          =>
      primitive(in => java.time.Month.valueOf(in.readString(null)), (a, out) => out.writeVal(a.toString))
    case StandardType.MonthDayType       => primitive(_.readMonthDay(null), (a, out) => out.writeVal(a))
    case StandardType.OffsetDateTimeType => primitive(_.readOffsetDateTime(null), (a, out) => out.writeVal(a))
    case StandardType.OffsetTimeType     => primitive(_.readOffsetTime(null), (a, out) => out.writeVal(a))
    case StandardType.PeriodType         => primitive(_.readPeriod(null), (a, out) => out.writeVal(a))
    case StandardType.YearType           => primitive(_.readYear(null), (a, out) => out.writeVal(a))
    case StandardType.YearMonthType      => primitive(_.readYearMonth(null), (a, out) => out.writeVal(a))
    case StandardType.ZonedDateTimeType  => primitive(_.readZonedDateTime(null), (a, out) => out.writeVal(a))
    case StandardType.ZoneIdType         => primitive(_.readZoneId(null), (a, out) => out.writeVal(a))
    case StandardType.ZoneOffsetType     => primitive(_.readZoneOffset(null), (a, out) => out.writeVal(a))
    case StandardType.CurrencyType       =>
      primitive(in => java.util.Currency.getInstance(in.readString(null)), (a, out) => out.writeVal(a.toString))
  }

  def optionCodec[A](schema: Schema[A], config: JsoniterCodec.Configuration): JsonValueCodec[Option[A]] = {
    if (schema.isInstanceOf[Schema.Record[_]] || schema.isInstanceOf[Schema.Enum[_]]) {
      new JsonValueCodec[Option[A]] {
        val codec = schemaCodec(schema, config)

        override def decodeValue(in: JsonReader, default: Option[A]): Option[A] = in.nextToken() match {
          case 'n' => in.readNullOrError(None, "expected null")
          case '{' =>
            if (!in.isNextToken('}')) {
              in.rollbackToken()
              in.rollbackToken()
              Some(codec.decodeValue(in, null.asInstanceOf[A]))
            } else None
          case _   =>
            in.rollbackToken()
            Some(codec.decodeValue(in, null.asInstanceOf[A]))
        }

        override def encodeValue(value: Option[A], out: JsonWriter): Unit = value match {
          case Some(value) => codec.encodeValue(value, out)
          case None        => out.writeNull()
        }

        override def nullValue: Option[A] = null.asInstanceOf[Option[A]]
      }
    } else {
      new JsonValueCodec[Option[A]] {
        val codec = schemaCodec(schema, config)

        override def decodeValue(in: JsonReader, default: Option[A]): Option[A] = in.nextToken() match {
          case 'n' => in.readNullOrError(None, "expected null")
          case _   =>
            in.rollbackToken()
            Some(codec.decodeValue(in, null.asInstanceOf[A]))
        }

        override def encodeValue(value: Option[A], out: JsonWriter): Unit = value match {
          case Some(value) => codec.encodeValue(value, out)
          case None        => out.writeNull()
        }

        override def nullValue: Option[A] = null.asInstanceOf[Option[A]]
      }
    }
  }

  def tupleCodec[A, B](
    left: Schema[A],
    right: Schema[B],
    config: JsoniterCodec.Configuration,
  ): JsonValueCodec[(A, B)] = {
    new JsonValueCodec[(A, B)] {
      val leftCodec  = schemaCodec(left, config)
      val rightCodec = schemaCodec(right, config)

      override def encodeValue(value: (A, B), out: JsonWriter): Unit = {
        out.writeArrayStart()
        leftCodec.encodeValue(value._1, out)
        rightCodec.encodeValue(value._2, out)
        out.writeArrayEnd()
      }

      override def decodeValue(in: JsonReader, default: (A, B)): (A, B) = {
        if (!in.isNextToken('[')) in.decodeError("expected '['")
        val a = leftCodec.decodeValue(in, null.asInstanceOf[A])
        if (!in.isNextToken(',')) in.commaError()
        val b = rightCodec.decodeValue(in, null.asInstanceOf[B])
        if (!in.isNextToken(']')) in.decodeError("expected ']'")
        else (a, b)
      }

      override def nullValue: (A, B) = null.asInstanceOf[(A, B)]
    }
  }

  def keyCodec[A](schema: Schema[A]): Option[JsonKeyCodec[A]] = schema match {
    case Schema.Primitive(StandardType.StringType, _) =>
      Some {
        new JsonKeyCodec[A] {
          override def decodeKey(in: JsonReader): A           = in.readKeyAsString()
          override def encodeKey(x: A, out: JsonWriter): Unit = out.writeKey(x.asInstanceOf[String])
        }
      }
    case Schema.Primitive(StandardType.CharType, _)   =>
      Some {
        new JsonKeyCodec[A] {
          override def decodeKey(in: JsonReader): A           = in.readKeyAsChar()
          override def encodeKey(x: A, out: JsonWriter): Unit = out.writeKey(x.asInstanceOf[Char])
        }
      }
    case Schema.Primitive(StandardType.BoolType, _)   =>
      Some {
        new JsonKeyCodec[A] {
          override def decodeKey(in: JsonReader): A           = in.readKeyAsBoolean()
          override def encodeKey(x: A, out: JsonWriter): Unit = out.writeKey(x.asInstanceOf[Boolean])
        }
      }
    case Schema.Primitive(StandardType.ByteType, _)   =>
      Some {
        new JsonKeyCodec[A] {
          override def decodeKey(in: JsonReader): A           = in.readKeyAsByte()
          override def encodeKey(x: A, out: JsonWriter): Unit = out.writeKey(x.asInstanceOf[Byte])
        }
      }
    case Schema.Primitive(StandardType.ShortType, _)  =>
      Some {
        new JsonKeyCodec[A] {
          override def decodeKey(in: JsonReader): A           = in.readKeyAsShort()
          override def encodeKey(x: A, out: JsonWriter): Unit = out.writeKey(x.asInstanceOf[Short])
        }
      }
    case Schema.Primitive(StandardType.IntType, _)    =>
      Some {
        new JsonKeyCodec[A] {
          override def decodeKey(in: JsonReader): A           = in.readKeyAsInt()
          override def encodeKey(x: A, out: JsonWriter): Unit = out.writeKey(x.asInstanceOf[Int])
        }
      }
    case Schema.Primitive(StandardType.LongType, _)   =>
      Some {
        new JsonKeyCodec[A] {
          override def decodeKey(in: JsonReader): A           = in.readKeyAsLong()
          override def encodeKey(x: A, out: JsonWriter): Unit = out.writeKey(x.asInstanceOf[Long])
        }
      }
    case Schema.Transform(c, f, g, a, _)              =>
      keyCodec(a.foldLeft(c)((s, a) => s.annotate(a))).map { codec =>
        new JsonKeyCodec[A] {
          override def decodeKey(in: JsonReader): A = f(codec.decodeKey(in)) match {
            case Left(reason) => throw new RuntimeException(s"failed to decode key $reason")
            case Right(value) => value
          }

          override def encodeKey(x: A, out: JsonWriter): Unit = g(x) match {
            case Left(reason) => throw new RuntimeException(s"Failed to encode key $x: $reason")
            case Right(value) => codec.encodeKey(value, out)
          }
        }
      }
    case Schema.Lazy(inner)                           => keyCodec(inner())
    case _                                            => None
  }

  def mapCodec[K, V](
    ks: Schema[K],
    vs: Schema[V],
    config: JsoniterCodec.Configuration,
  ): JsonValueCodec[Map[K, V]] = keyCodec(ks) match {
    case Some(keyCodec) =>
      new JsonValueCodec[Map[K, V]] {
        val valueCodec: JsonValueCodec[V] = schemaCodec(vs, config)

        override def decodeValue(in: JsonReader, default: Map[K, V]): Map[K, V] = {
          if (!in.isNextToken('{')) in.decodeError("expected '{'")
          if (in.isNextToken('}')) Map.empty[K, V]
          else {
            in.rollbackToken()
            val kvs = new java.util.LinkedHashMap[K, V](8)
            while ({
              kvs.put(keyCodec.decodeKey(in), valueCodec.decodeValue(in, null.asInstanceOf[V]))
              in.isNextToken(',')
            }) ()
            if (!in.isCurrentToken('}')) in.decodeError("expected '}'")
            import scala.jdk.CollectionConverters._
            kvs.asScala.toMap
          }
        }

        override def encodeValue(map: Map[K, V], out: JsonWriter): Unit = {
          out.writeObjectStart()
          map.foreach { case (key, value) =>
            keyCodec.encodeKey(key, out)
            valueCodec.encodeValue(value, out)
          }
          out.writeObjectEnd()
        }

        override def nullValue: Map[K, V] = null.asInstanceOf[Map[K, V]]
      }

    case None =>
      new JsonValueCodec[Map[K, V]] {
        val codec = chunkCodec(tupleCodec(ks, vs, config))

        override def decodeValue(in: JsonReader, default: Map[K, V]): Map[K, V] =
          codec.decodeValue(in, null.asInstanceOf[Chunk[(K, V)]]).toMap

        override def encodeValue(map: Map[K, V], out: JsonWriter): Unit =
          codec.encodeValue(Chunk.fromIterable(map), out)

        override def nullValue: Map[K, V] = null.asInstanceOf[Map[K, V]]
      }
  }

  def nonEmptyMapCodec[K, V](
    ks: Schema[K],
    vs: Schema[V],
    config: JsoniterCodec.Configuration,
  ): JsonValueCodec[NonEmptyMap[K, V]] =
    new JsonValueCodec[NonEmptyMap[K, V]] {
      val codec = mapCodec(ks, vs, config)

      override def decodeValue(in: JsonReader, default: NonEmptyMap[K, V]): NonEmptyMap[K, V] =
        NonEmptyMap.fromMapOption(codec.decodeValue(in, null)) match {
          case Some(value) => value
          case None        => in.decodeError("NonEmptyMap cannot be empty")
        }
      override def encodeValue(value: NonEmptyMap[K, V], out: JsonWriter): Unit               =
        codec.encodeValue(value.toMap, out)
      override def nullValue: NonEmptyMap[K, V] = null.asInstanceOf[NonEmptyMap[K, V]]
    }

  def transformCodec[A, B](
    schema: Schema[B],
    f: B => Either[String, A],
    g: A => Either[String, B],
    annotations: Chunk[Any],
    config: JsoniterCodec.Configuration,
    discriminator: Option[String],
  ): JsonValueCodec[A] =
    new JsonValueCodec[A] {
      val codec = schemaCodec(annotations.foldLeft(schema)((s, a) => s.annotate(a)), config, discriminator)

      override def decodeValue(in: JsonReader, default: A): A =
        f(codec.decodeValue(in, null.asInstanceOf[B])) match {
          case Left(reason) => in.decodeError(reason)
          case Right(value) => value
        }

      override def encodeValue(value: A, out: JsonWriter): Unit = g(value) match {
        case Left(_)      => out.writeNull()
        case Right(value) => codec.encodeValue(value, out)
      }

      override def nullValue: A = null.asInstanceOf[A]
    }

  def eitherCodec[A, B](
    left: Schema[A],
    right: Schema[B],
    config: JsoniterCodec.Configuration,
  ): JsonValueCodec[Either[A, B]] =
    new JsonValueCodec[Either[A, B]] {
      val leftCodec  = schemaCodec(left, config)
      val rightCodec = schemaCodec(right, config)

      override def encodeValue(value: Either[A, B], out: JsonWriter): Unit = {
        out.writeObjectStart()
        value match {
          case Left(a)  => out.writeKey("Left"); leftCodec.encodeValue(a, out)
          case Right(a) => out.writeKey("Right"); rightCodec.encodeValue(a, out)
        }
        out.writeObjectEnd()
      }

      override def decodeValue(in: JsonReader, default: Either[A, B]): Either[A, B] = {
        if (!in.isNextToken('{')) in.decodeError("expected '{'")
        val result = in.readKeyAsString() match {
          case "Left"  => Left(leftCodec.decodeValue(in, null.asInstanceOf[A]))
          case "Right" => Right(rightCodec.decodeValue(in, null.asInstanceOf[B]))
        }
        if (!in.isNextToken('}')) in.decodeError("expected '}'")
        result
      }

      override def nullValue: Either[A, B] = null.asInstanceOf[Either[A, B]]
    }

  def fallbackCodec[A, B](
    left: Schema[A],
    right: Schema[B],
    config: JsoniterCodec.Configuration,
    fullDecode: Boolean,
  ): JsonValueCodec[Fallback[A, B]] =
    new JsonValueCodec[Fallback[A, B]] {
      val leftCodec  = schemaCodec(left, config)
      val rightCodec = schemaCodec(right, config)

      override def decodeValue(in: JsonReader, default: Fallback[A, B]): Fallback[A, B] = {
        var left: Option[A]  = None
        var right: Option[B] = None

        if (in.isNextToken('[')) { // it's both
          try left = Some(leftCodec.decodeValue(in, null.asInstanceOf[A]))
          catch {
            case ex if NonFatal(ex) =>
              in.rollbackToken()
              in.skip()
          }
          if (!in.isNextToken(',')) in.commaError()
          if (fullDecode) {
            try right = Some(rightCodec.decodeValue(in, null.asInstanceOf[B]))
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
          try left = Some(leftCodec.decodeValue(in, null.asInstanceOf[A]))
          catch {
            case _: Throwable =>
              in.rollbackToMark()
              right = Some(rightCodec.decodeValue(in, null.asInstanceOf[B]))
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

      override def encodeValue(value: Fallback[A, B], out: JsonWriter): Unit = value match {
        case Fallback.Left(a)    => leftCodec.encodeValue(a, out)
        case Fallback.Right(b)   => rightCodec.encodeValue(b, out)
        case Fallback.Both(a, b) =>
          out.writeArrayStart()
          leftCodec.encodeValue(a, out)
          rightCodec.encodeValue(b, out)
          out.writeArrayEnd()
      }

      override def nullValue: Fallback[A, B] = null.asInstanceOf[Fallback[A, B]]
    }

  def lazyCodec[A](
    schema: Schema.Lazy[A],
    config: JsoniterCodec.Configuration,
    discriminator: Option[String],
  ): JsonValueCodec[A] =
    new JsonValueCodec[A] {
      lazy val codec = schemaCodec(schema.schema, config, discriminator)

      override def decodeValue(in: JsonReader, default: A): A   = codec.decodeValue(in, null.asInstanceOf[A])
      override def encodeValue(value: A, out: JsonWriter): Unit = codec.encodeValue(value, out)
      override def nullValue: A                                 = null.asInstanceOf[A]
    }

  private def isEmptyOptionalValue(
    schema: Schema.Field[_, _],
    value: Any,
    config: JsoniterCodec.Configuration,
  ): Boolean = {
    (!config.explicitEmptyCollections.encoding || schema.optional) && (value match {
      case None            => true
      case it: Iterable[_] => it.isEmpty
      case _               => false
    })
  }

  def recordCodec[Z](
    schema: Schema.GenericRecord,
    config: JsoniterCodec.Configuration,
    discriminator: Option[String],
  ): JsonValueCodec[ListMap[String, Any]] = discriminator match {
    case Some(discriminator) =>
      new JsonValueCodec[ListMap[String, Any]] {

        val len             = schema.fields.length
        val fields          = schema.fields.toArray
        val codecs          =
          fields.map(field => schemaCodec(field.schema.asInstanceOf[Schema[Any]], config))
        val names           = new Array[String](len)
        val fieldsWithCodec = new java.util.HashMap[String, (String, JsonValueCodec[Any])](fields.size << 1)

        var i = 0
        fields.foreach { field =>
          val codec = schemaCodec(field.schema, config).asInstanceOf[JsonValueCodec[Any]]
          val name  =
            if (config.fieldNameFormat == NameFormat.Identity) field.fieldName
            else if (field.fieldName == field.name) config.fieldNameFormat(field.fieldName)
            else field.fieldName
          names(i) = name
          fieldsWithCodec.put(name, (name, codec))
          field.aliases.foreach(fieldsWithCodec.put(_, (name, codec)))
          i += 1
        }

        val explicitEmptyCollections = config.explicitEmptyCollections.decoding
        val explicitNulls            = config.explicitNullValues.decoding
        val rejectExtraFields        = schema.rejectExtraFields || config.rejectExtraFields

        override def decodeValue(in: JsonReader, default: ListMap[String, Any]): ListMap[String, Any] = {
          var continue = !in.isNextToken('}')
          in.rollbackToken()
          if (!continue) return ListMap.empty[String, Any]
          val map      = new java.util.HashMap[String, Any](fields.length << 1)
          while (continue) {
            val fieldNameOrAlias = in.readKeyAsString()
            val fieldWithCodec   = fieldsWithCodec.get(fieldNameOrAlias)
            if (fieldWithCodec ne null) {
              val (fieldName, codec) = fieldWithCodec
              val prev               = map.put(fieldName, codec.decodeValue(in, null.asInstanceOf[Any]))
              if (prev != null) in.decodeError(s"duplicate field $fieldNameOrAlias")
            } else if (!rejectExtraFields || discriminator.contains(fieldNameOrAlias)) {
              in.skip()
            } else in.decodeError(s"extra field $fieldNameOrAlias")
            continue = in.isNextToken(',')
          }
          in.rollbackToken()
          var idx      = 0
          while (idx < fields.length) {
            val field     = fields(idx)
            val fieldName = names(idx)
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
                      case collection: Schema.Collection[_, _] if !explicitEmptyCollections => collection.empty
                      case _: Schema.Optional[_] if !explicitNulls                          => None
                      case _                                                                =>
                        in.decodeError(s"missing field $fieldName")
                    }
                  }
                },
              )
            }
            idx += 1
          }
          (ListMap.newBuilder[String, Any] ++= ({ // to avoid O(n) insert operations
            import scala.collection.JavaConverters.mapAsScalaMapConverter // use deprecated class for Scala 2.12 compatibility

            map.asScala
          }: @scala.annotation.nowarn)).result()
        }

        override def encodeValue(xs: ListMap[String, Any], out: JsonWriter): Unit = {
          var i = 0
          while (i < fields.length) {
            val schema = fields(i)
            if (!schema.transient) {
              val key   = names(i)
              val value = xs(key)
              if (
                !isEmptyOptionalValue(
                  schema,
                  value,
                  config,
                ) && ((value != null && value != None) || config.explicitNullValues.encoding)
              ) {
                out.writeKey(key)
                codecs(i).encodeValue(value, out)
              }
            }
            i += 1
          }
        }

        override def nullValue: ListMap[String, Any] = null.asInstanceOf[ListMap[String, Any]]
      }

    case None =>
      new JsonValueCodec[ListMap[String, Any]] {

        val len             = schema.fields.length
        val fields          = schema.fields.toArray
        val codecs          =
          fields.map(field => schemaCodec(field.schema.asInstanceOf[Schema[Any]], config))
        val names           = new Array[String](len)
        val fieldsWithCodec = new java.util.HashMap[String, (String, JsonValueCodec[Any])](fields.size << 1)

        var i = 0
        fields.foreach { field =>
          val codec = schemaCodec(field.schema, config).asInstanceOf[JsonValueCodec[Any]]
          val name  =
            if (config.fieldNameFormat == NameFormat.Identity) field.fieldName
            else if (field.fieldName == field.name) config.fieldNameFormat(field.fieldName)
            else field.fieldName
          names(i) = name
          fieldsWithCodec.put(name, (name, codec))
          field.aliases.foreach(fieldsWithCodec.put(_, (name, codec)))
          i += 1
        }

        val explicitEmptyCollections = config.explicitEmptyCollections.decoding
        val explicitNulls            = config.explicitNullValues.decoding
        val rejectExtraFields        = schema.rejectExtraFields || config.rejectExtraFields

        override def decodeValue(in: JsonReader, default: ListMap[String, Any]): ListMap[String, Any] = {
          if (!in.isNextToken('{')) in.decodeError("expected '{'")
          var continue = !in.isNextToken('}')
          if (continue) in.rollbackToken()
          val map      = new java.util.HashMap[String, Any](fields.length << 1)
          while (continue) {
            val fieldNameOrAlias = in.readKeyAsString()
            val fieldWithCodec   = fieldsWithCodec.get(fieldNameOrAlias)
            if (fieldWithCodec ne null) {
              val (fieldName, codec) = fieldWithCodec
              val prev               = map.put(fieldName, codec.decodeValue(in, null.asInstanceOf[Any]))
              if (prev != null) in.decodeError(s"duplicate field $fieldNameOrAlias")
            } else if (!rejectExtraFields) {
              in.skip()
            } else in.decodeError(s"extra field $fieldNameOrAlias")
            continue = in.isNextToken(',')
          }
          if (!in.isCurrentToken('}')) in.decodeError("expected '}'")
          var idx      = 0
          while (idx < fields.length) {
            val field     = fields(idx)
            val fieldName = names(idx)
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
                      case collection: Schema.Collection[_, _] if !explicitEmptyCollections => collection.empty
                      case _: Schema.Optional[_] if !explicitNulls                          => None
                      case _                                                                =>
                        in.decodeError(s"missing field $fieldName")
                    }
                  }
                },
              )
            }
            idx += 1
          }
          (ListMap.newBuilder[String, Any] ++= ({ // to avoid O(n) insert operations
            import scala.collection.JavaConverters.mapAsScalaMapConverter // use deprecated class for Scala 2.12 compatibility

            map.asScala
          }: @scala.annotation.nowarn)).result()
        }

        override def encodeValue(xs: ListMap[String, Any], out: JsonWriter): Unit = {
          out.writeObjectStart()
          var i = 0
          while (i < fields.length) {
            val schema = fields(i)
            if (!schema.transient) {
              val key   = names(i)
              val value = xs(key)
              if (
                !isEmptyOptionalValue(
                  schema,
                  value,
                  config,
                ) && ((value != null && value != None) || config.explicitNullValues.encoding)
              ) {
                out.writeKey(key)
                codecs(i).encodeValue(value, out)
              }
            }
            i += 1
          }
          out.writeObjectEnd()
        }

        override def nullValue: ListMap[String, Any] = null.asInstanceOf[ListMap[String, Any]]
      }
  }

  // scalafmt: { maxColumn = 400, optIn.configStyleArguments = false }
  def caseClassCodec[Z](schema: Schema.Record[Z], config: JsoniterCodec.Configuration, discriminator: Option[String]): JsonValueCodec[Z] = {

    discriminator match {
      case None =>
        new JsonValueCodec[Z] {

          val len     = schema.fields.length
          val fields  = new Array[Schema.Field[Z, _]](len)
          val codecs  = new Array[JsonValueCodec[Any]](len)
          val names   = new Array[String](len)
          val aliases = new java.util.HashMap[String, Int](len << 1)

          var i = 0
          schema.fields.foreach { field =>
            fields(i) = field
            codecs(i) = schemaCodec(field.schema, config).asInstanceOf[JsonValueCodec[Any]]
            val name =
              if (config.fieldNameFormat == NameFormat.Identity) field.fieldName
              else if (field.fieldName == field.name) config.fieldNameFormat(field.fieldName)
              else field.fieldName
            names(i) = name
            aliases.put(name, i)
            field.aliases.foreach(aliases.put(_, i))
            i += 1
          }

          val explicitEmptyCollections = config.explicitEmptyCollections.decoding
          val explicitNulls            = config.explicitNullValues.decoding
          val rejectExtraFields        = schema.rejectExtraFields || config.rejectExtraFields

          override def decodeValue(in: JsonReader, default: Z): Z = {
            if (!in.isNextToken('{')) in.decodeError("expected '{'")
            var continue = !in.isNextToken('}')
            if (continue) in.rollbackToken()
            val len      = fields.length
            val buffer   = new Array[Any](len)
            while (continue) {
              val key = in.readKeyAsString()
              val idx = aliases.getOrDefault(key, -1)
              if (idx >= 0) {
                if (buffer(idx) == null) buffer(idx) = codecs(idx).decodeValue(in, null.asInstanceOf[Any])
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
                    case collection: Schema.Collection[_, _] if !explicitEmptyCollections => collection.empty
                    case _: Schema.Optional[_] if !explicitNulls                          => None
                    case _                                                                =>
                      in.decodeError(s"missing field ${names(idx)}")
                  }
                }
              }
              idx += 1
            }
            Unsafe.unsafe { implicit unsafe: Unsafe =>
              schema.construct(Chunk.fromArray(buffer)) match {
                case Left(message) => in.decodeError(message)
                case Right(record) => record
              }
            }
          }

          override def encodeValue(x: Z, out: JsonWriter): Unit = {
            out.writeObjectStart()
            var idx = 0
            while (idx < fields.length) {
              val schema = fields(idx)
              if (!schema.transient) {
                val value = schema.get(x)
                if (!isEmptyOptionalValue(schema, value, config) && ((value != null && value != None) || config.explicitNullValues.encoding)) {
                  out.writeKey(names(idx))
                  codecs(idx).encodeValue(value, out)
                }
              }
              idx += 1
            }
            out.writeObjectEnd()
          }

          override def nullValue: Z = null.asInstanceOf[Z]
        }

      case Some(discriminator) =>
        new JsonValueCodec[Z] {

          val len     = schema.fields.length
          val fields  = new Array[Schema.Field[Z, _]](len)
          val codecs  = new Array[JsonValueCodec[Any]](len)
          val names   = new Array[String](len)
          val aliases = new java.util.HashMap[String, Int](len << 1)

          var i = 0
          schema.fields.foreach { field =>
            fields(i) = field
            codecs(i) = schemaCodec(field.schema, config).asInstanceOf[JsonValueCodec[Any]]
            val name =
              if (config.fieldNameFormat == NameFormat.Identity) field.fieldName
              else if (field.fieldName == field.name) config.fieldNameFormat(field.fieldName)
              else field.fieldName
            names(i) = name
            aliases.put(name, i)
            field.aliases.foreach(aliases.put(_, i))
            i += 1
          }
          aliases.put(discriminator, i)

          val explicitEmptyCollections = config.explicitEmptyCollections.decoding
          val explicitNulls            = config.explicitNullValues.decoding
          val rejectExtraFields        = schema.rejectExtraFields || config.rejectExtraFields

          override def decodeValue(in: JsonReader, default: Z): Z = {
            var continue = !in.isNextToken('}')
            if (continue) in.rollbackToken()
            val len      = fields.length
            val buffer   = new Array[Any](len)
            while (continue) {
              val key = in.readKeyAsString()
              val idx = aliases.getOrDefault(key, -1)
              if (idx >= 0) {
                if (idx == len) in.skip() // skipping discriminator field values
                else if (buffer(idx) == null) buffer(idx) = codecs(idx).decodeValue(in, null.asInstanceOf[Any])
                else in.decodeError(s"duplicate field $key")
              } else if (!rejectExtraFields) in.skip()
              else in.decodeError(s"extra field $key")
              continue = in.isNextToken(',')
            }
            if (!in.isCurrentToken('}')) in.decodeError("expected '}'")
            in.rollbackToken()
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
                    case collection: Schema.Collection[_, _] if !explicitEmptyCollections => collection.empty
                    case _: Schema.Optional[_] if !explicitNulls                          => None
                    case _                                                                =>
                      in.decodeError(s"missing field ${names(idx)}")
                  }
                }
              }
              idx += 1
            }
            Unsafe.unsafe { implicit unsafe: Unsafe =>
              schema.construct(Chunk.fromArray(buffer)) match {
                case Left(message) => in.decodeError(message)
                case Right(record) => record
              }
            }
          }

          override def encodeValue(x: Z, out: JsonWriter): Unit = {
            var idx = 0
            while (idx < fields.length) {
              val schema = fields(idx)
              if (!schema.transient) {
                val value = schema.get(x)
                if (!isEmptyOptionalValue(schema, value, config) && ((value != null && value != None) || config.explicitNullValues.encoding)) {
                  out.writeKey(names(idx))
                  codecs(idx).encodeValue(value, out)
                }
              }
              idx += 1
            }
          }

          override def nullValue: Z = null.asInstanceOf[Z]
        }
    }
  }

  def enumCodec[Z](schema: Schema.Enum[Z], config: JsoniterCodec.Configuration): JsonValueCodec[Z] = {

    def format(caseName: String): String =
      if (config.discriminatorFormat == NameFormat.Identity) caseName
      else config.discriminatorFormat(caseName)

    val caseNameAliases = new mutable.HashMap[String, Schema.Case[Z, Any]]
    schema.cases.foreach { case_ =>
      val schema = case_.asInstanceOf[Schema.Case[Z, Any]]
      caseNameAliases.put(schema.caseName, schema)
      schema.caseNameAliases.foreach { alias => caseNameAliases.put(alias, schema) }
    }

    // if simpleEnum annotation is present and/or all cases are CaseClass0, its mapped as String
    if (
      schema.annotations.exists(_.isInstanceOf[simpleEnum]) ||
      !schema.cases.exists(c => !c.schema.isInstanceOf[Schema.CaseClass0[_]])
    ) {
      new JsonValueCodec[Z] {
        val decodingCases = new java.util.HashMap[String, Z](caseNameAliases.size << 1)
        caseNameAliases.foreach { case (name, _case) =>
          decodingCases.put(format(name), _case.schema.asInstanceOf[Schema.CaseClass0[Z]].defaultConstruct())
        }
        val encodingCases = new java.util.HashMap[Z, String](schema.cases.size << 1)
        schema.nonTransientCases.foreach { _case =>
          encodingCases.put(_case.schema.asInstanceOf[Schema.CaseClass0[Z]].defaultConstruct(), format(_case.caseName))
        }

        override def encodeValue(value: Z, out: JsonWriter): Unit = {
          val caseName = encodingCases.getOrDefault(value, null)
          if (caseName eq null) out.encodeError(s"encoding non-matching enum case $value")
          out.writeVal(caseName)
        }
        override def decodeValue(in: JsonReader, default: Z): Z   = {
          val subtype = in.readString(null)
          val result  = decodingCases.get(subtype)
          if (result == null) in.decodeError(s"unrecognized subtype $subtype")
          result
        }
        override def nullValue: Z                                 = null.asInstanceOf[Z]
      }
    } else if (schema.noDiscriminator || config.noDiscriminator) {
      new JsonValueCodec[Z] {
        val cases  = schema.cases.toArray
        val codecs =
          cases.map(c => schemaCodec(c.schema, config).asInstanceOf[JsonValueCodec[Any]])

        override def decodeValue(in: JsonReader, default: Z): Z = {
          val it = codecs.iterator
          while (it.hasNext) {
            in.setMark()
            try return it.next().decodeValue(in, null).asInstanceOf[Z]
            catch { case ex if NonFatal(ex) => in.rollbackToMark() }
          }
          in.decodeError("none of the subtypes could decode the data")
        }

        override def encodeValue(value: Z, out: JsonWriter): Unit = {
          var idx = 0
          while (idx < cases.length) {
            val _case = cases(idx)
            if (_case.isCase(value)) {
              codecs(idx).encodeValue(value, out)
              return
            }
            idx += 1
          }
          out.writeObjectStart() // for transient case
          out.writeObjectEnd()
        }

        override def nullValue: Z = null.asInstanceOf[Z]
      }
    } else {
      val discriminator = schema.discriminatorName.orElse(config.discriminatorName)
      discriminator match {
        case None =>
          new JsonValueCodec[Z] {
            val cases        = schema.cases.toArray
            val keys         = cases.map { _case => format(_case.caseName) }
            val codecsLookup = new java.util.HashMap[String, JsonValueCodec[Any]](caseNameAliases.size << 1)
            val codecsArray  = cases.map { _case =>
              val codec = schemaCodec(_case.schema.asInstanceOf[Schema[Any]], config)
              codecsLookup.put(format(_case.caseName), codec)
              _case.caseNameAliases.foreach(cna => codecsLookup.put(cna, codec))
              codec
            }

            override def decodeValue(in: JsonReader, default: Z): Z = {
              if (in.nextToken() != '{') in.decodeError("expected '{'")
              if (in.isNextToken('}')) in.decodeError("missing subtype")
              in.rollbackToken()
              val key     = in.readKeyAsString()
              val codec   = codecsLookup.getOrDefault(key, null)
              if (codec eq null) in.decodeError(s"unrecognized subtype $key")
              val decoded = codec.decodeValue(in, null).asInstanceOf[Z]
              if (in.nextToken() != '}') in.decodeError("expected '}'")
              decoded
            }

            override def encodeValue(value: Z, out: JsonWriter): Unit = {
              var idx = 0
              while (idx < cases.length) {
                val _case = cases(idx)
                if (_case.isCase(value)) {
                  out.writeObjectStart()
                  if (!_case.transient) {
                    out.writeKey(keys(idx))
                    codecsArray(idx).encodeValue(value, out)
                  }
                  out.writeObjectEnd()
                  return
                }
                idx += 1
              }
              out.writeObjectStart() // for transient cases
              out.writeObjectEnd()
            }

            override def nullValue: Z = null.asInstanceOf[Z]
          }

        case Some(discriminator) =>
          new JsonValueCodec[Z] {
            val cases        = schema.cases.toArray
            val keys         = cases.map { _case => format(_case.caseName) }
            val codecsLookup = new java.util.HashMap[String, JsonValueCodec[Any]](caseNameAliases.size << 1)
            val codecsArray  = cases.map { _case =>
              val codec = schemaCodec(_case.schema.asInstanceOf[Schema[Any]], config, Some(discriminator))
              codecsLookup.put(format(_case.caseName), codec)
              _case.caseNameAliases.foreach(cna => codecsLookup.put(cna, codec))
              codec
            }

            override def decodeValue(in: JsonReader, default: Z): Z = {
              if (!in.isNextToken('{')) in.decodeError("expected '{'")
              in.setMark()
              if (in.isNextToken('}')) in.decodeError(s"missing subtype $discriminator")
              in.rollbackToken()
              while ({
                if (in.readKeyAsString() != discriminator) {
                  in.skip()
                  in.nextToken() match {
                    case '}' => in.decodeError(s"missing subtype $discriminator")
                    case ',' => true
                    case _   => in.commaError()
                  }
                } else false
              }) ()
              val subtype = in.readString(null)
              in.rollbackToMark()
              val codec   = codecsLookup.get(subtype)
              if (codec eq null) in.decodeError(s"unrecognized subtype $subtype")
              val result  = codec.decodeValue(in, null).asInstanceOf[Z]
              if (!in.isNextToken('}')) in.decodeError("expected '}'")
              result
            }

            override def encodeValue(value: Z, out: JsonWriter): Unit = {
              var idx = 0
              while (idx < cases.length) {
                val _case = cases(idx)
                if (_case.isCase(value) && !_case.transient) {
                  out.writeObjectStart()
                  out.writeKey(discriminator)
                  out.writeVal(keys(idx))
                  codecsArray(idx).encodeValue(value, out)
                  out.writeObjectEnd()
                  return
                }
                idx += 1
              }
              out.writeObjectStart() // for transient cases
              out.writeObjectEnd()
            }

            override def nullValue: Z = null.asInstanceOf[Z]
          }
      }
    }
  }

  private def isEmptyArray(value: DynamicValue): Boolean = value match {
    case DynamicValue.Sequence(xs) if xs.isEmpty => true
    case DynamicValue.SetValue(xs) if xs.isEmpty => true
    case _                                       => false
  }

  private def isEmptyValue(value: DynamicValue): Boolean = value match {
    case DynamicValue.NoneValue => true
    case _                      => false
  }

  def dynamicCodec(schema: Schema.Dynamic, config: JsoniterCodec.Configuration): JsonValueCodec[DynamicValue] = {
    if (schema.annotations.exists(_.isInstanceOf[directDynamicMapping])) {
      new JsonValueCodec[DynamicValue] { codec =>
        override def decodeValue(in: JsonReader, default: DynamicValue): DynamicValue = {
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
                vs(i) = codec.decodeValue(in, null)
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
                kvs.put(in.readKeyAsString(), codec.decodeValue(in, null))
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

        override def encodeValue(value: DynamicValue, out: JsonWriter): Unit = value match {
          case DynamicValue.Record(_, values)              =>
            out.writeObjectStart()
            values.foreach { case (key, value) =>
              if (
                (config.explicitEmptyCollections.encoding || !isEmptyArray(value)) &&
                (config.explicitNullValues.encoding || !isEmptyValue(value))
              ) {
                out.writeKey(key)
                codec.encodeValue(value, out)
              }
            }
            out.writeObjectEnd()
          case DynamicValue.Enumeration(_, _)              =>
            throw new Exception(s"DynamicValue.Enumeration is not supported in directDynamicMapping mode")
          case DynamicValue.Sequence(values)               => chunkCodec(codec).encodeValue(values, out)
          case DynamicValue.Dictionary(_)                  =>
            throw new Exception(s"DynamicValue.Dictionary is not supported in directDynamicMapping mode")
          case DynamicValue.SetValue(values)               => chunkCodec(codec).encodeValue(Chunk.fromIterable(values), out)
          case DynamicValue.Primitive(value, standardType) => primitiveCodec(standardType).encodeValue(value, out)
          case DynamicValue.Singleton(_)                   =>
            out.writeObjectStart()
            out.writeObjectEnd()
          case DynamicValue.SomeValue(value)               => codec.encodeValue(value, out)
          case DynamicValue.NoneValue                      => out.writeNull()
          case DynamicValue.Tuple(_, _)                    =>
            throw new Exception(s"DynamicValue.Tuple is not supported in directDynamicMapping mode")
          case DynamicValue.LeftValue(_)                   =>
            throw new Exception(s"DynamicValue.LeftValue is not supported in directDynamicMapping mode")
          case DynamicValue.RightValue(_)                  =>
            throw new Exception(s"DynamicValue.RightValue is not supported in directDynamicMapping mode")
          case DynamicValue.BothValue(left, right)         =>
            out.writeArrayStart()
            codec.encodeValue(left, out)
            codec.encodeValue(right, out)
            out.writeArrayEnd()
          case DynamicValue.DynamicAst(_)                  =>
            throw new Exception(s"DynamicValue.DynamicAst is not supported in directDynamicMapping mode")
          case DynamicValue.Error(message)                 =>
            throw new Exception(message)
        }

        override def nullValue: DynamicValue = null.asInstanceOf[DynamicValue]
      }
    } else {
      schemaCodec(DynamicValue.schema, config)
    }
  }
}
