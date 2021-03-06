package timeline;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import socialcore.activitymanager.db.RecordBase;
import socialcore.activitymanager.db.hbase.HBaseRecordBuilder;
import socialcore.activitymanager.model.Atividade;
import util.AtividadeBuilder;
import util.AtividadeComparator;
import util.HBaseHelper;

public class AppHomeTimelineExample {
	
	private static final String COLUMN_FAMILY_INFO = "info";
	private static final String TABLE_NAME_ACTIVITIES = "atividades";
	private static final long APP = 2L;

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

		long minStamp = System.currentTimeMillis() * RecordBase.KEY_SHIFT_MULTIPLIER;;
		
	    AtividadeBuilder.anAtividade().withUsuario(3L).withApp(1L).withCorpo("Usuario 3; App1;").create();
	    AtividadeBuilder.anAtividade().withUsuario(3L).withApp(2L).withCorpo("Usuario 3; App2;").create();

	    AtividadeBuilder.anAtividade().withUsuario(2L).withApp(1L).withCorpo("Usuario 2; App1;").create();
		AtividadeBuilder.anAtividade().withUsuario(2L).withApp(2L).withCorpo("Usuario 2; App2;").create();
		
	    AtividadeBuilder.anAtividade().withUsuario(4L).withApp(1L).withCorpo("Usuario 4; App1;").create();
	    AtividadeBuilder.anAtividade().withUsuario(4L).withApp(2L).withCorpo("Usuario 4; App2;").create();
		
		AtividadeBuilder.anAtividade().withUsuario(1L).withApp(1L).withCorpo("Usuario 1; App1;").create();
		AtividadeBuilder.anAtividade().withUsuario(1L).withApp(2L).withCorpo("Usuario 1; App2;").create();
		
		long maxStamp = System.currentTimeMillis() * RecordBase.KEY_SHIFT_MULTIPLIER + RecordBase.KEY_SALT_RANDOM_MAX_VALUE;
		
		HTable table = new HTable(conf, TABLE_NAME_ACTIVITIES);
		Scan scan = new Scan();
		List<Filter> filters = new ArrayList<Filter>();
		
		scan.addFamily(Bytes.toBytes(COLUMN_FAMILY_INFO));
		scan.setTimeRange(minStamp, maxStamp);
	    
	    StringBuilder builder = new StringBuilder("\\d+-");
	    builder.append(StringUtils.leftPad(String.valueOf(APP), 20, '0'));
	    builder.append("-");
	    builder.append(".+");

	    filters.add(new RowFilter(CompareOp.EQUAL, new RegexStringComparator(builder.toString())));
	    FilterList filterList = new FilterList(filters);
	    scan.setFilter(filterList);
	    
	    
	    ResultScanner scanner = table.getScanner(scan);
	    
	    List<Atividade> atividades = new ArrayList<Atividade>();
	    Set<Atividade> atividadesSet = new TreeSet<Atividade>(new AtividadeComparator());
	     
	    for (Result result : scanner) {
	    	Atividade atividade = HBaseRecordBuilder.builderFor(Atividade.class).toRecord(result);
	    	atividades.add(atividade);
	    	atividadesSet.add(atividade);
	    	
	    	System.out.println(atividade.getPublishedAt().getTime() + " " + atividade.getResultado().getFlexibleFields("corpo"));
		}
	    System.out.println("*****");
	    
	    Collections.sort(atividades, new AtividadeComparator());
	    for (Atividade atividade : atividades) {
	    	System.out.println(atividade.getPublishedAt().getTime() + " " + atividade.getResultado().getFlexibleFields("corpo"));
		}
	    
	    System.out.println("*****");
	    
	    for (Atividade atividade : atividadesSet) {
	    	System.out.println(atividade.getPublishedAt().getTime() + " " + atividade.getResultado().getFlexibleFields("corpo"));
		}
	    
	}
}
