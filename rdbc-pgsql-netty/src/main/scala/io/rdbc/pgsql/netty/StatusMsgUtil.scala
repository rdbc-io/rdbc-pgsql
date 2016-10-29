package io.rdbc.pgsql.netty

import io.rdbc.pgsql.core.messages.backend.{ErrorMessage, StatusMessage}

object StatusMsgUtil {

  def isFatal(err: ErrorMessage): Boolean = {
    if (err.statusData.sqlState == "57014") {
      false //query canceled
    } else {
      val errCat = err.statusData.sqlState.take(2)
      errCat match {
        case "57" => true //operator intervention
        case "58" => true //system error
        case "XX" => true //PG internal error
        case _ => false
      }
    }
  }

  def isWarning(statusMsg: StatusMessage): Boolean = {
    val sqlState = statusMsg.statusData.sqlState
    sqlState.startsWith("01") || sqlState.startsWith("02")
  }

}
