import com.sbartnik.common.CassandraOperations

object InitCassandraRunner {
  def main(args: Array[String]) {
    CassandraOperations.getInitializedSession
    print("Done.")
  }
}