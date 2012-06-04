package timeline;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import socialcore.activitymanager.db.hbase.HBaseRecordBuilder;
import socialcore.activitymanager.model.Atividade;
import util.AtividadeBuilder;
import util.HBaseHelper;

public class UserAppHomeTimelineExample {
	
	private static final String COLUMN_FAMILY_INFO = "info";
	private static final String TABLE_NAME_ACTIVITIES = "atividades";
	private static final long USER = 1L;
	private static final long APP = 2L;
	private static final Map<Long, List<Long>> SEGUIDOS = new HashMap<Long, List<Long>>();
	
//	static{
//		for(long user = 1; user <=  AtividadeBuilder.TOTAL_USERS; user++){
//			List<Long> seguidos = new ArrayList<Long>();
//			
//			for(long seguidor = 1; seguidor <= AtividadeBuilder.TOTAL_USERS; seguidor++){
//				if(user == seguidor){
//					continue;
//				}
//	
//				seguidos.add(Long.valueOf(seguidor));			
//			}
//			
//			SEGUIDOS.put(user, seguidos);		
//		}
//	}
	

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		HBaseHelper helper = HBaseHelper.getHelper(conf);

		/*
		 * Clear database
		 */
	    helper.dropTable(TABLE_NAME_ACTIVITIES);
	    helper.createTable(TABLE_NAME_ACTIVITIES, COLUMN_FAMILY_INFO);
	    
		/*
		 * Para o exemplo, temos 2 produtos, 4 usuários. Cada usuário realiza 1 atividade 
		 * em cada produto.
		 */
		AtividadeBuilder.anAtividade().withUsuario(1L).withApp(1L).withCorpo("Usuario 1; App1;").create();
		AtividadeBuilder.anAtividade().withUsuario(1L).withApp(2L).withCorpo("Usuario 1; App2;").create();

		AtividadeBuilder.anAtividade().withUsuario(1L).withApp(1L).withCorpo("Usuario 1; App1;").create();
		AtividadeBuilder.anAtividade().withUsuario(1L).withApp(2L).withCorpo("Usuario 1; App2;").create();

		AtividadeBuilder.anAtividade().withUsuario(1L).withApp(1L).withCorpo("Usuario 1; App1;").create();
		AtividadeBuilder.anAtividade().withUsuario(1L).withApp(2L).withCorpo("Usuario 1; App2;").create();

		long minStamp = System.currentTimeMillis();
		
	    AtividadeBuilder.anAtividade().withUsuario(3L).withApp(1L).withCorpo("Usuario 3; App1;").create();
	    AtividadeBuilder.anAtividade().withUsuario(3L).withApp(2L).withCorpo("Usuario 3; App2;").create();

	    AtividadeBuilder.anAtividade().withUsuario(2L).withApp(1L).withCorpo("Usuario 2; App1;").create();
		AtividadeBuilder.anAtividade().withUsuario(2L).withApp(2L).withCorpo("Usuario 2; App2;").create();
		
	    AtividadeBuilder.anAtividade().withUsuario(4L).withApp(1L).withCorpo("Usuario 4; App1;").create();
	    AtividadeBuilder.anAtividade().withUsuario(4L).withApp(2L).withCorpo("Usuario 4; App2;").create();
		
		AtividadeBuilder.anAtividade().withUsuario(1L).withApp(1L).withCorpo("Usuario 1; App1;").create();
		AtividadeBuilder.anAtividade().withUsuario(1L).withApp(2L).withCorpo("Usuario 1; App2;").create();
		
		long maxStamp = System.currentTimeMillis();
		
		List<Long> seguidosUsuario1 = new ArrayList<Long>();
		seguidosUsuario1.add(2L);
		seguidosUsuario1.add(3L);
		seguidosUsuario1.add(4L);
		
		List<Long> seguidosUsuario2 = new ArrayList<Long>();
		seguidosUsuario1.add(3L);
		
		List<Long> seguidosUsuario3 = new ArrayList<Long>();
		seguidosUsuario1.add(2L);
		
		
		SEGUIDOS.put(1L, seguidosUsuario1);
		SEGUIDOS.put(2L, seguidosUsuario2);
		SEGUIDOS.put(3L, seguidosUsuario3);

		/*
		 * 
		 */
		HTable table = new HTable(conf, TABLE_NAME_ACTIVITIES);
		Scan scan = new Scan();
		byte[] family = Bytes.toBytes(COLUMN_FAMILY_INFO);
		byte[] qualifier = Bytes.toBytes("usuario");
		List<Filter> filters = new ArrayList<Filter>();
		
		scan.addFamily(Bytes.toBytes(COLUMN_FAMILY_INFO));
//		scan.setTimeRange(minStamp, maxStamp);
	    
	    List<Long> seguidos = SEGUIDOS.get(USER);
	    StringBuilder builder = new StringBuilder("^(");
	    builder.append(StringUtils.leftPad(String.valueOf(USER), 20, '0'));
	    
	    for (Long seguido : seguidos) {
	    	String seguidoAsString = StringUtils.leftPad(String.valueOf(seguido), 20, '0');
		    builder.append("|");
		    builder.append(seguidoAsString);
//	    	filters.add(new SingleColumnValueFilter(family, qualifier , CompareOp.EQUAL, new BinaryComparator(value)));
		}
	    builder.append(")-");
	    builder.append(StringUtils.leftPad(String.valueOf(APP), 20, '0'));
	    builder.append("-");
	    builder.append(".+");
//	    
//	    filters.add(new SingleColumnValueFilter(family, qualifier , CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(USER))));
	    filters.add(new RowFilter(CompareOp.EQUAL, new RegexStringComparator(builder.toString())));
	    
	    
	    
//	    filters.add(new SingleColumnValueFilter(family, Bytes.toBytes("app") , CompareOp.EQUAL, Bytes.toBytes(APP)));
	    PageFilter pageFilter = new PageFilter(3);
	    filters.add(pageFilter);

	    
	    FilterList filterList = new FilterList(filters);
	    scan.setFilter(filterList);
	    
	    
	    
	    ResultScanner scanner = table.getScanner(scan);
	    
	    List<Atividade> atividades = new ArrayList<Atividade>();
	    Set<Atividade> atividadesSet = new TreeSet<Atividade>();
	     
	    for (Result result : scanner) {
	    	Atividade atividade = HBaseRecordBuilder.builderFor(Atividade.class).toRecord(result);
	    	atividades.add(atividade);
	    	atividadesSet.add(atividade);
	    	
	    	System.out.println(atividade.getPublishedAt().getTime() + " " + atividade.getResultado().getFlexibleFields("corpo"));
		}
	    System.out.println("*****");
	    
	    Collections.sort(atividades);
	    for (Atividade atividade : atividades) {
	    	System.out.println(atividade.getPublishedAt().getTime() + " " + atividade.getResultado().getFlexibleFields("corpo"));
		}
	    
	    System.out.println("*****");
	    
	    for (Atividade atividade : atividadesSet) {
	    	System.out.println(atividade.getPublishedAt().getTime() + " " + atividade.getResultado().getFlexibleFields("corpo"));
		}
	    
//	    System.out.println(atividades);
//	    System.out.println(atividades.size());

	}
	
//	public static void example() throws IOException {
//		clearDatabase();
//	    System.out.println("Adding rows to table...");
//	    helper.fillTableAtividades("atividades", 1, 100, 1, 20, true, false, "info");
//
//	    HTable table = new HTable(conf, "atividades");
//
//	    // vv RowFilterExample
//	    Scan scan = new Scan();
//	    scan.addFamily(Bytes.toBytes("info"));
//
//	    Filter filter1 = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, // co RowFilterExample-1-Filter1 Create filter, while specifying the comparison operator and comparator. Here an exact match is needed.
//	    	      new BinaryComparator(Bytes.toBytes("00000000000000000001-00000000000000000222")));
//	    scan.setFilter(filter1);
//	    ResultScanner scanner1 = table.getScanner(scan);
//	    // ^^ RowFilterExample
//	    System.out.println("Scanning table #1...");
//	    // vv RowFilterExample
//	    for (Result res : scanner1) {
//	      System.out.println(res);
//	    }
//	    scanner1.close();
//
//	    Filter filter2 = new RowFilter(CompareFilter.CompareOp.EQUAL, // co RowFilterExample-2-Filter2 Another filter, this time using a regular expression to match the row keys.
//	      new RegexStringComparator("[0-9]{20}-[0-9]{17}222"));
//	    scan.setFilter(filter2);
//	    ResultScanner scanner2 = table.getScanner(scan);
//	    // ^^ RowFilterExample
//	    System.out.println("Scanning table #2...");
//	    // vv RowFilterExample
//	    for (Result res : scanner2) {
//	      System.out.println(res);
//	    }
//	    scanner2.close();
//
//	    Filter filter3 = new RowFilter(CompareFilter.CompareOp.EQUAL, // co RowFilterExample-3-Filter3 The third filter uses a substring match approach.
//	      new SubstringComparator("00000000000000000001-00000000000000000123"));
//	    scan.setFilter(filter3);
//	    ResultScanner scanner3 = table.getScanner(scan);
//	    // ^^ RowFilterExample
//	    System.out.println("Scanning table #3...");
//	    // vv RowFilterExample
//	    for (Result res : scanner3) {
//	      System.out.println(res);
//	    }
//	    scanner3.close();
//	    // ^^ RowFilterExample
//	  }
}
