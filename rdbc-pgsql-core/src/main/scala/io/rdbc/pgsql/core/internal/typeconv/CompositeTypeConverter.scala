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

package io.rdbc.pgsql.core.internal.typeconv

import io.rdbc.pgsql.core.typeconv.{PartialTypeConverter, TypeConverter}
import io.rdbc.sapi.exceptions.ConversionException
import io.rdbc.util.Preconditions.checkNotNull

import scala.util.{Failure, Success, Try}

private[core] class CompositeTypeConverter(partialConverters: Vector[PartialTypeConverter[_]])
  extends TypeConverter {

  private val totalConverters: Map[Class[_], TypeConverter] = {
    partialConverters
      .groupBy(_.cls)
      .map { case (targetClass, singleTargetConverters) =>
        val totalCoercer = partialConvertersToSingleTotal(singleTargetConverters)
        (targetClass, totalCoercer)
      }.toMap
  }

  private def partialConvertersToSingleTotal(partials: Vector[PartialTypeConverter[_]]): TypeConverter = {
    new TypeConverter {
      def convert[T](value: Any, targetType: Class[T]): Try[T] = {
        val partialsChained = partials.foldLeft(Option.empty[Any]) {
          (acc, converter) =>
            acc.orElse(converter.convert(value))
        }

        partialsChained.map(Success(_))
          .getOrElse(Failure(new ConversionException(value, targetType)))
          .asInstanceOf[Try[T]]
      }
    }
  }

  def convert[T](value: Any, targetType: Class[T]): Try[T] = {
    checkNotNull(value)
    if (targetType.equals(value.getClass)) {
      Success(value.asInstanceOf[T])
    } else {
      totalConverters.get(targetType).map { coercer =>
        coercer.convert(value, targetType)
      }.getOrElse(Failure(new ConversionException(value, targetType)))
    }
  }
}
