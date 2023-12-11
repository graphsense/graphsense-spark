package org.graphsense

trait Job {

  def run(from: Option[Integer], to: Option[Integer]): Unit

}
