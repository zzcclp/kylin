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

package tech.mlsql.service

import java.nio.charset.Charset
import scala.collection.JavaConverters._
import org.apache.http.client.fluent.{Form, Request}
import tech.mlsql.utils.PathFun
import org.apache.kylin.common.KylinConfig

case class SQLHeadHint(t: String, body:String, input: Option[String], output: Option[String],
                       params: Map[String, String])

object MlsqlService {

  def parseHint(query: String): SQLHeadHint = {
    val headers = query.split("\n").filter(item=>
      item.stripMargin.startsWith("--%") || item.stripMargin.startsWith("#%")
    ).map{item=>
      item.stripMargin.stripPrefix("--%").stripPrefix("#%")
    }

    val body = query.split("\n").
      filterNot(_.stripMargin.startsWith("--%")).
      filterNot(_.stripMargin.startsWith("#%")).mkString("\n")

    var t: String = ""
    var input: Option[String] = None
    var output: Option[String] = None
    val headerParams = scala.collection.mutable.HashMap[String, String]()
    headers.foreach { header =>
      if (!header.contains("=")) {
        t = header
      }else {
        val Array(k, v) = header.split("=", 2)
        k match {
          case "input" =>
            input = Some(v)
          case "output" =>
            output = Some(v)
          case _ =>
            headerParams += (k -> v)
        }
      }
    }
    SQLHeadHint(t, body, input, output, headerParams.toMap)
  }

  def executeScript(query: String, header: SQLHeadHint): String = {
    val options = KylinConfig.getInstanceFromEnv.getMlsqlConfigOverride.asScala
    val owner = header.params.getOrElse("owner", options("user_name"))
    val home = header.params.getOrElse("engine_home", options("engine_home"))

    val url = header.params.getOrElse("url", options("mlsql_engine_url"))
    val formParamBuilder = Form.form()
    (header.params - "url" - "owner").foreach(item => formParamBuilder.add(item._1, item._2))
    formParamBuilder.add("sql", header.body)
    formParamBuilder.add("owner", owner)
    formParamBuilder.add("defaultPathPrefix", PathFun(home).add(owner).toPath)
    formParamBuilder.add("async", "false")
    //val output = header.output.getOrElse(UUID.randomUUID().toString.replaceAll("-", ""))
    //s"run command as ShowFunctionsExt.`` as ${output};"

    val returnContent = Request.Post(url.stripSuffix("/") + "/run/script").
      bodyForm(formParamBuilder.build(), Charset.forName("utf-8")).
      execute().returnContent()
    new String(returnContent.asBytes(), Charset.forName("utf-8"));
  }
}
