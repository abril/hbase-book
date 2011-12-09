package filters;

// cc RowFilterExample Example using a filter to select specific rows
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

public class AtividadeRowFilterExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("atividades");
    helper.createTable("atividades", "info");
    System.out.println("Adding rows to table...");
    helper.fillTableAtividades("atividades", 1, 100, 1, 20, true, false, "info");

    HTable table = new HTable(conf, "atividades");

    // vv RowFilterExample
    Scan scan = new Scan();
    scan.addFamily(Bytes.toBytes("info"));

    Filter filter1 = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, // co RowFilterExample-1-Filter1 Create filter, while specifying the comparison operator and comparator. Here an exact match is needed.
    	      new BinaryComparator(Bytes.toBytes("00000000000000000001-00000000000000000222")));
    scan.setFilter(filter1);
    ResultScanner scanner1 = table.getScanner(scan);
    // ^^ RowFilterExample
    System.out.println("Scanning table #1...");
    // vv RowFilterExample
    for (Result res : scanner1) {
      System.out.println(res);
    }
    scanner1.close();

    Filter filter2 = new RowFilter(CompareFilter.CompareOp.EQUAL, // co RowFilterExample-2-Filter2 Another filter, this time using a regular expression to match the row keys.
      new RegexStringComparator("[0-9]{20}-[0-9]{17}222"));
    scan.setFilter(filter2);
    ResultScanner scanner2 = table.getScanner(scan);
    // ^^ RowFilterExample
    System.out.println("Scanning table #2...");
    // vv RowFilterExample
    for (Result res : scanner2) {
      System.out.println(res);
    }
    scanner2.close();

    Filter filter3 = new RowFilter(CompareFilter.CompareOp.EQUAL, // co RowFilterExample-3-Filter3 The third filter uses a substring match approach.
      new SubstringComparator("00000000000000000001-00000000000000000123"));
    scan.setFilter(filter3);
    ResultScanner scanner3 = table.getScanner(scan);
    // ^^ RowFilterExample
    System.out.println("Scanning table #3...");
    // vv RowFilterExample
    for (Result res : scanner3) {
      System.out.println(res);
    }
    scanner3.close();
    // ^^ RowFilterExample
  }
}
