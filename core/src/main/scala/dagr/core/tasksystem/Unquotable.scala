package dagr.core.tasksystem

case class Unquotable(any: Any) {
  override def toString: String = any.toString
}
