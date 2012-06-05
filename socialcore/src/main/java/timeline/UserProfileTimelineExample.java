package timeline;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

import socialcore.activitymanager.db.hbase.HBaseRecordBuilder;
import socialcore.activitymanager.model.Atividade;
import socialcore.resources.exceptions.InternalServerErrorException;
import util.AtividadeBuilder;
import util.AtividadeComparator;
import util.HBaseHelper;

public class UserProfileTimelineExample {
	private static final String COLUMN_FAMILY_INFO = "info";
	private static final String TABLE_NAME_ACTIVITIES = "atividades";
	private static final long USER = 1L;

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		HBaseHelper helper = HBaseHelper.getHelper(conf);

	    clearDatabase(helper);
	    
		/*
		 * Para o exemplo, temos 8 produtos, 4 usuários. 
		 */
		populateDatabase();

		List<Atividade> atividades = queryDatabase(conf);
		
		System.out.println("*********");
	    
	    Collections.sort(atividades, new AtividadeComparator());
	    for (Atividade atividade : atividades) {
	    	System.out.println(atividade.getPublishedAt().getTime() + " " + atividade.getResultado().getFlexibleFields("corpo"));
		}

	}

	private static List<Atividade> queryDatabase(Configuration conf) throws IOException {
		HTable table = new HTable(conf, TABLE_NAME_ACTIVITIES);
		Scan scan = new Scan();
		List<Filter> filters = new ArrayList<Filter>();
		
		scan.addFamily(Bytes.toBytes(COLUMN_FAMILY_INFO));
		byte[] startRow = Bytes.toBytes(StringUtils.leftPad(String.valueOf(USER), 20, '0'));
		scan.setStartRow(startRow);
		byte[] stopRow = Bytes.toBytes(StringUtils.leftPad(String.valueOf(USER + 1), 20, '0'));
		scan.setStopRow(stopRow);
		
	    StringBuilder builder = new StringBuilder("^(");
	    
	    builder.append(StringUtils.leftPad(String.valueOf(USER), 20, '0'));
	    builder.append(")-");
	    builder.append(".+");

	    filters.add(new RowFilter(CompareOp.EQUAL, new RegexStringComparator(builder.toString())));
	    FilterList filterList = new FilterList(filters);
	    scan.setFilter(filterList);
	    
	    ResultScanner scanner = table.getScanner(scan);
	    
	    List<Atividade> atividades = new ArrayList<Atividade>();
	     
	    for (Result result : scanner) {
	    	Atividade atividade = HBaseRecordBuilder.builderFor(Atividade.class).toRecord(result);
	    	atividades.add(atividade);	    	
	    	System.out.println(atividade.getPublishedAt().getTime() + " " + atividade.getResultado().getFlexibleFields("corpo"));
		}
		return atividades;
	}

	private static void populateDatabase() throws InternalServerErrorException {
		AtividadeBuilder.anAtividade().withUsuario(1L).withApp(1L).withCorpo("Usuario 1; App1;").create();
		AtividadeBuilder.anAtividade().withUsuario(1L).withApp(2L).withCorpo("Usuario 1; App2;").create();

		AtividadeBuilder.anAtividade().withUsuario(1L).withApp(3L).withCorpo("Usuario 1; App3;").create();
		AtividadeBuilder.anAtividade().withUsuario(1L).withApp(4L).withCorpo("Usuario 1; App4;").create();

		AtividadeBuilder.anAtividade().withUsuario(1L).withApp(5L).withCorpo("Usuario 1; App5;").create();
		AtividadeBuilder.anAtividade().withUsuario(1L).withApp(6L).withCorpo("Usuario 1; App6;").create();
		
	    AtividadeBuilder.anAtividade().withUsuario(3L).withApp(1L).withCorpo("Usuario 3; App1;").create();
	    AtividadeBuilder.anAtividade().withUsuario(3L).withApp(2L).withCorpo("Usuario 3; App2;").create();

	    AtividadeBuilder.anAtividade().withUsuario(2L).withApp(1L).withCorpo("Usuario 2; App1;").create();
		AtividadeBuilder.anAtividade().withUsuario(2L).withApp(2L).withCorpo("Usuario 2; App2;").create();
		
	    AtividadeBuilder.anAtividade().withUsuario(4L).withApp(1L).withCorpo("Usuario 4; App1;").create();
	    AtividadeBuilder.anAtividade().withUsuario(4L).withApp(2L).withCorpo("Usuario 4; App2;").create();
		
		AtividadeBuilder.anAtividade().withUsuario(1L).withApp(7L).withCorpo("Usuario 1; App7;").create();
		AtividadeBuilder.anAtividade().withUsuario(1L).withApp(8L).withCorpo("Usuario 1; App8;").create();
	}

	private static void clearDatabase(HBaseHelper helper) throws IOException {
		helper.dropTable(TABLE_NAME_ACTIVITIES);
	    helper.createTable(TABLE_NAME_ACTIVITIES, COLUMN_FAMILY_INFO);
	}
}
