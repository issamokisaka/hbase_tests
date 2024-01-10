import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import java.nio.file.{Files, Paths}

class Main(connectionFactory: Connection) {

}

object Main {
  private def getHbase(table:Table, rk:String, column:String, value:String)={
    val scan = new Get(Bytes.toBytes(rk))
    val scanner = table.get(scan)
    val result = if (scanner.isEmpty) null else scanner.getValue(Bytes.toBytes(column), Bytes.toBytes(value))
    result
  }

  private def readHbase(connection: Connection): Unit = {
    val tableName = "big.hb_sbs_chatbot"
    val table = connection.getTable(TableName.valueOf(tableName))
    val chave = "5511806019110"
    val scan = getHbase(table, chave, "f", "dados")
    println(new String(scan))
  }

  private def sendFile(connection: Connection): Unit = {
    val tableName = "big.hb_binary_files"
    val table = connection.getTable(TableName.valueOf(tableName))
    val put = new Put(Bytes.toBytes("1"))
    val fileContent = Files.readAllBytes(Paths.get("/home/issamo/Documents/regras_modelos_para_fontes_cluster_5_1.0/TesteHbase/sun.png"))
    put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("binary_data"), fileContent)

    table.put(put)
  }

  private def getFile(connection: Connection): Unit = {
    val tableName = "big.hb_binary_files"
    val table = connection.getTable(TableName.valueOf(tableName))
    val get = new Get(Bytes.toBytes("1"))
    val result = table.get(get)
    val fileContent = result.getValue(Bytes.toBytes("f"), Bytes.toBytes("binary_data"))
    Files.write(Paths.get("/tmp/saida.png"), fileContent)

  }

  def main(args: Array[String]): Unit = {
    val zookeeper = "pxl1big00133,pxl1big00134,pxl1big00140"
    val hbconf = HBaseConfiguration.create()
    hbconf.set("hbase.zookeeper.quorum", zookeeper)
    val connection = ConnectionFactory.createConnection(hbconf)

//    readHbase(connection)
//    sendFile(connection)
    getFile(connection)
  }
}
