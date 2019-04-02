package com.criteo.cuttle.flow


case class Signal(name : String)

object SignalK {
  def apply(name: String): Signal = new Signal(name)
  def produceFor(wfId : String)(s : Signal) = ???

}

// Signal("