/*
 * Copyright 2016 rdbc contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rdbc.pgsql.core.internal

import java.time._
import java.util.UUID

import io.rdbc.pgsql.core.internal.typeconv.javaconv._
import io.rdbc.pgsql.core.internal.typeconv.pgvalconv._
import io.rdbc.pgsql.core.typeconv.TypeMapping.ArrowAssoc
import io.rdbc.pgsql.core.typeconv.{PartialTypeConverter, TypeMapping}
import io.rdbc.pgsql.core.types.{PgType, _}
import io.rdbc.sapi._
import scodec.bits.ByteVector

package object typeconv {
  private[core] val BuiltInTypeConverters = Vector[PartialTypeConverter[_]](
    BoolTypeConverter,
    JavaBooleanTypeConverter,
    PgBoolTypeConverter,

    ByteTypeConverter,
    JavaByteTypeConverter,

    ShortTypeConverter,
    JavaShortTypeConverter,
    PgInt2TypeConverter,

    IntTypeConverter,
    JavaIntegerTypeConverter,
    PgInt4TypeConverter,

    LongTypeConverter,
    JavaLongTypeConverter,
    PgInt8TypeConverter,

    FloatTypeConverter,
    JavaFloatTypeConverter,
    PgFloat4TypeConverter,

    DoubleTypeConverter,
    JavaDoubleTypeConverter,
    PgFloat8TypeConverter,

    DecimalNumberTypeConverter,
    PgNumericTypeConverter,
    BigDecimalTypeConverter,
    JavaBigDecimalTypeConverter,

    StringTypeConverter,
    PgTextTypeConverter,
    PgCharTypeConverter,
    PgVarcharTypeConverter,

    InstantTypeConverter,
    PgTimestampTypeConverter,

    LocalDateTypeConverter,
    PgDateTypeConverter,

    LocalDateTimeTypeConverter,

    LocalTimeTypeConverter,
    PgTimeTypeConverter,

    ZonedDateTimeTypeConverter,
    OffsetDateTimeTypeConverter,
    PgTimestampTzTypeConverter,

    ByteArrTypeConverter,
    ByteVectorTypeConverter,
    PgByteaTypeConverter,

    UuidTypeConverter,
    PgUuidTypeConverter
  )

  private[core] val BuiltInTypeMappings = {
    Vector[TypeMapping[_, _ <: PgType[_ <: PgVal[_]]]](
      classOf[SqlVarchar] -> PgVarcharType,
      classOf[SqlNVarchar] -> PgVarcharType,

      classOf[SqlChar] -> PgCharType,
      classOf[SqlNChar] -> PgCharType,

      classOf[String] -> PgTextType,
      classOf[SqlClob] -> PgTextType,
      classOf[SqlNClob] -> PgTextType,

      classOf[SqlBinary] -> PgByteaType,
      classOf[SqlVarbinary] -> PgByteaType,
      classOf[SqlBlob] -> PgByteaType,

      classOf[Boolean] -> PgBoolType,
      classOf[java.lang.Boolean] -> PgBoolType,
      classOf[SqlBoolean] -> PgBoolType,

      classOf[DecimalNumber.Val] -> PgNumericType,
      DecimalNumber.NaN.getClass -> PgNumericType,
      DecimalNumber.PosInfinity.getClass -> PgNumericType,
      DecimalNumber.NegInfinity.getClass -> PgNumericType,
      classOf[SqlDecimal] -> PgNumericType,
      classOf[SqlNumeric] -> PgNumericType,
      classOf[BigDecimal] -> PgNumericType,
      classOf[java.math.BigDecimal] -> PgNumericType,

      classOf[Byte] -> PgInt2Type,
      classOf[java.lang.Byte] -> PgInt2Type,

      classOf[Short] -> PgInt2Type,
      classOf[java.lang.Short] -> PgInt2Type,
      classOf[SqlSmallInt] -> PgInt2Type,

      classOf[Int] -> PgInt4Type,
      classOf[java.lang.Integer] -> PgInt4Type,
      classOf[SqlInteger] -> PgInt4Type,

      classOf[Long] -> PgInt8Type,
      classOf[java.lang.Long] -> PgInt8Type,
      classOf[SqlBigInt] -> PgInt8Type,

      classOf[Float] -> PgFloat4Type,
      classOf[java.lang.Float] -> PgFloat4Type,
      classOf[SqlFloat] -> PgFloat4Type,
      classOf[SqlReal] -> PgFloat4Type,

      classOf[Double] -> PgFloat8Type,
      classOf[java.lang.Double] -> PgFloat8Type,
      classOf[SqlDouble] -> PgFloat8Type,

      classOf[LocalDate] -> PgDateType,
      classOf[SqlDate] -> PgDateType,

      classOf[LocalTime] -> PgTimeType,
      classOf[SqlTime] -> PgTimeType,

      classOf[LocalDateTime] -> PgTimestampType,
      classOf[SqlTimestamp] -> PgTimestampType,
      classOf[Instant] -> PgTimestampType,

      classOf[ZonedDateTime] -> PgTimestampTzType,
      classOf[SqlTimestampTz] -> PgTimestampTzType,

      classOf[Array[Byte]] -> PgByteaType,
      classOf[ByteVector] -> PgByteaType,

      classOf[UUID] -> PgUuidType
    )
  }

  private[typeconv] implicit class DecimalNumberToScala(val dn: io.rdbc.japi.DecimalNumber)
    extends AnyVal {
    def toScala: DecimalNumber = {
      if (dn.isNaN) {
        DecimalNumber.NaN
      } else if (dn.isNegInifinity) {
        DecimalNumber.NegInfinity
      } else if (dn.isPosInifinity) {
        DecimalNumber.PosInfinity
      } else {
        DecimalNumber.Val(dn.getValue)
      }
    }
  }


}
