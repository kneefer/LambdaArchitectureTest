package com.sbartnik.layers.serving

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server._
import akka.stream.ActorMaterializer
import com.kneefer.util.HttpUtil
import com.sbartnik.common.Helpers
import com.sbartnik.domain.ActionBySite
import com.sbartnik.layers.serving.logic.{LambdaBusinessLogic, RdbmsBusinessLogic}

import scala.concurrent.Future
import scala.language.postfixOps
import scala.util.{Failure => FutureFailure, Success => FutureSuccess}

object LambdaRestServer extends App with HttpUtil with Helpers {

  implicit val system = ActorSystem("LambdaRestServer")
  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher

  def execute[T <: AnyRef](api: String, exFunc: (BusinessLogic => T)) = {
    val businessLogic = api match {
      case "rdbms" => RdbmsBusinessLogic
      case "lambda" => LambdaBusinessLogic
    }
    logDuration(onSuccess(Future(write(exFunc(businessLogic))))(complete(_)))
  }

  val routes: Route = get {
    pathPrefix(Segment) { api =>
      // siteName - if passed, result will contain site actions only of provided site
      //          - if not passed, result will consist of actions of all sites
      // windowSize - if passed, result will include aggregated actions for period of time equals N*windowLength
      //            - if not passed, result will include aggregated actions for full available history
      path("siteActions") {
        parameters('siteName ? "", 'windowSize ? -1) { (siteName, windowSize) =>
          execute(api, _.getSiteActions(siteName, windowSize.toLong))
        }
      }

      // siteName - if passed, result will contain unique visitors only of provided site
      //          - if not passed, result will consist of unique visitors of all sites
      // windowIndex - if passed, result will include aggregated actions for period of time equals N*windowLength
      //            - if not passed, result will include aggregated actions for full available history
      path("uniqueVisitors") {
        parameters('siteName ? "", 'windowIndex ? -1) { (siteName, windowIndex) =>
          execute(api, _.getUniqueVisitors(siteName, windowIndex.toLong))
        }
      }
    }
  }

  val binding = Http().bindAndHandle(routes, "localhost", 9999)

  binding.onComplete {
    case FutureSuccess(binding) =>
      val localAddress = binding.localAddress
      println(s"Server available on ${localAddress.getHostName}:${localAddress.getPort}")
    case FutureFailure(err) =>
      logger.error(s"Binding failed. Error: ${err.getMessage}")
      system.terminate()
  }
}
