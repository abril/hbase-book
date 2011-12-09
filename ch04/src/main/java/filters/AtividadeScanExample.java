package filters;

// cc ScanExample Example using a filter to select specific rows
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

public class AtividadeScanExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("atividades");
    helper.createTable("atividades", "info");
    System.out.println("Adding rows to table...");
    helper.fillTableAtividades("atividades", 1, 100, 1, 20, true, false, "info");

    HTable table = new HTable(conf, "atividades");

    // vv ScanExample
    Scan scan = new Scan();
    scan.addFamily(Bytes.toBytes("info"));

    scan.setStartRow(Bytes.toBytes("00000000000000000001-00000000000000000123"));
    scan.setStopRow(Bytes.toBytes("00000000000000000001-00000000000000000124"));
    ResultScanner scanner1 = table.getScanner(scan);
    // ^^ ScamExample
    System.out.println("Scanning table #1...");
    // vv ScanExample
    for (Result res : scanner1) {
      System.out.println(res);
    }
    scanner1.close();

    scan.setStartRow(Bytes.toBytes("00000000000000000001-00000000000000000222"));
    scan.setStopRow(Bytes.toBytes("00000000000000000001-00000000000000000223"));
    ResultScanner scanner2 = table.getScanner(scan);
    // ^^ ScanExample
    System.out.println("Scanning table #2...");
    // vv ScanExample
    for (Result res : scanner2) {
      System.out.println(res);
    }
    scanner2.close();
    // ^^ ScanExample
  }
}
