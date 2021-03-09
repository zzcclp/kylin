/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tech.mlsql.utils

import scala.collection.mutable.ArrayBuffer

/**
 * 2019-04-25 WilliamZhu(allwefantasy@gmail.com)
 */
class PathFun(rootPath: String) {
  private val buffer = new ArrayBuffer[String]()
  buffer += rootPath.stripSuffix("/")

  def add(path: String) = {
    val cleanPath = path.stripPrefix("/").stripSuffix("/")
    if (!cleanPath.isEmpty) {
      buffer += cleanPath
    }
    this
  }

  def /(path: String) = {
    add(path)
  }

  def toPath = {
    buffer.mkString("/")
  }

}

object PathFun {
  def apply(rootPath: String): PathFun = new PathFun(rootPath)

  def joinPath(rootPath: String, paths: String*) = {
    val pf = apply(rootPath)
    for (arg <- paths) pf.add(arg)
    pf.toPath
  }
}
