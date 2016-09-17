/*
 * Copyright 2016 Krzysztof Pado
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

package io.rdbc

import java.lang.reflect.InvocationTargetException

import com.typesafe.config.Config
import com.typesafe.config.ConfigException.BadValue

package object pgsql {

  implicit class RichConfig(val underlying: Config) extends AnyVal {
    def getOptionalConfig(path: String): Option[Config] = if (underlying.hasPath(path)) {
      Some(underlying.getConfig(path))
    } else {
      None
    }

    def getClassInstance[T](path: String, constructorArg: Config): T = {
      val fqcn = underlying.getString(path)
      try {
        Class.forName(fqcn).getConstructor(classOf[Config]).newInstance(constructorArg).asInstanceOf[T]
      } catch {
        case ex@(_: ClassCastException | _: ClassNotFoundException) => throw new BadValue(path, s"Class name '$fqcn' could not be instantiated as target type", ex)
        case ex: InstantiationException => throw new BadValue(path, s"Class '$fqcn' has to declare a constructor with single Config parameter", ex)
        case ex: InvocationTargetException => throw ex.getCause
      }
    } //TODO code dupl

    def getClassInstances[T](path: String): Seq[T] = {
      import scala.collection.JavaConversions._

      underlying.getStringList(path).toList.map { fqcn =>
        try {
          Class.forName(fqcn).newInstance().asInstanceOf[T]
        } catch {
          case ex@(_: ClassCastException | _: ClassNotFoundException) => throw new BadValue(path, s"Class name '$fqcn' could not be instantiated as target type", ex)
          case ex: InstantiationException => throw new BadValue(path, s"Class '$fqcn' has to declare a non-arg constructor", ex)
        }
      }
    }


    def getClassInstance[T](path: String): T = {
      val fqcn = underlying.getString(path)
      try {
        Class.forName(fqcn).newInstance().asInstanceOf[T]
      } catch {
        case ex@(_: ClassCastException | _: ClassNotFoundException) => throw new BadValue(path, s"Class name '$fqcn' could not be instantiated as target type", ex)
        case ex: InstantiationException => throw new BadValue(path, s"Class '$fqcn' has to declare a non-arg constructor", ex)
      }
    }
  }

}
