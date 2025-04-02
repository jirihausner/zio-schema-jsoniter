# zio-schema-jsoniter

`zio-schema-jsoniter` seamlessly integrates [zio-schema](https://github.com/zio/zio-schema) with [plokhotnyuk's jsoniter-scala](https://github.com/plokhotnyuk/jsoniter-scala), a safe and ultra-fast JSON library.

![CI Badge](https://github.com/jirihausner/zio-schema-jsoniter/actions/workflows/ci.yml/badge.svg?branch=main) ![Maven Central Version](https://img.shields.io/maven-central/v/io.github.jirihausner/zio-schema-jsoniter_2.13) [![Scala Steward badge](https://img.shields.io/badge/Scala_Steward-helping-blue.svg?style=flat&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA4AAAAQCAMAAAARSr4IAAAAVFBMVEUAAACHjojlOy5NWlrKzcYRKjGFjIbp293YycuLa3pYY2LSqql4f3pCUFTgSjNodYRmcXUsPD/NTTbjRS+2jomhgnzNc223cGvZS0HaSD0XLjbaSjElhIr+AAAAAXRSTlMAQObYZgAAAHlJREFUCNdNyosOwyAIhWHAQS1Vt7a77/3fcxxdmv0xwmckutAR1nkm4ggbyEcg/wWmlGLDAA3oL50xi6fk5ffZ3E2E3QfZDCcCN2YtbEWZt+Drc6u6rlqv7Uk0LdKqqr5rk2UCRXOk0vmQKGfc94nOJyQjouF9H/wCc9gECEYfONoAAAAASUVORK5CYII=)](https://github.com/scala-steward-org/scala-steward) [![ZIO Schema Jsoniter](https://img.shields.io/github/stars/jirihausner/zio-schema-jsoniter?style=social)](https://github.com/jirihausner/zio-schema-jsoniter)

## Why zio-schema-jsoniter?

- Perfect for projects that already use `jsoniter-scala` that want to take advantage of the type-safe schema definitions of `zio-schema`.
- Provides an alternative to [zio-schema-json](https://github.com/zio/zio-schema/tree/main/zio-schema-json), catering to teams already invested in `jsoniter-scala` ecosystem.
- Makes it easier to gradually migrate to `zio-schema` or incorporate its features into legacy stacks.

## Installation

In order to use this library, you need to add one following line in your `build.sbt` file:

```scala
libraryDependencies += "io.github.jirihausner" %% "zio-schema-jsoniter" % "0.1.0"
```

## Example

```scala
import zio.schema.{DeriveSchema, Schema}

case class Person(name: String, age: Int)

object Person {
  implicit val schema: Schema[Person] = DeriveSchema.gen
}

// derive `JsonValueCodec[A]` from implicit `Schema[A]`
import com.github.plokhotnyuk.jsoniter_scala.core.{JsonValueCodec, readFromString, writeToString}
import zio.schema.codec.jsoniter.JsoniterCodec

implicit val codec: JsonValueCodec[Person] = JsoniterCodec.schemaJsonValueCodec(Person.schema)

readFromString[Person]("""{"name": "John", "age": 30}""") // Person("John", 30)
writeToString(Person("Adam", 24))                         // "{"name":"Adam","age":24}"

// derive `BinaryCodec[A]` from implicit `JsonValueCodec[A]`
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
import zio.schema.codec.jsoniter.JsoniterCodec.jsonValueBinaryCodec

jsonValueBinaryCodec[Person](JsonCodecMaker.make[Person]) // zio.schema.codec.BinaryCodec[Person]

// derive `BinaryCodec[A]` backed by `JsonValueCodec[A]` from implicit `Schema[A]` directly
import zio.schema.codec.jsoniter.JsoniterCodec.schemaBasedBinaryCodec

schemaBasedBinaryCodec[Person](Person.schema) // zio.schema.codec.BinaryCodec[Person]
```

## Acknowledgements

This library was heavily inspired by [zio-schema-json](https://github.com/zio/zio-schema/tree/main/zio-schema-json). Huge thanks to its original contributors for laying foundational ideas and implementation, which greatly influenced `zio-schema-jsoniter`.

## Disclaimer

`zio-schema-jsoniter` is not intended to compete with `zio-schema-json`. Instead, it serves as a complementary option for developers who prefer or already use `jsoniter-scala` in their stack.

---

Contributions are welcome! If you have suggestions, improvements, or feature requests, feel free to open an issue or a pull request.
