package mapreduce;

// cc AnalyzeData MapReduce job that reads the imported data and analyzes it.
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import socialcore.activitymanager.db.hbase.HBaseRecordBuilder;
import socialcore.activitymanager.model.Atividade;
import socialcore.resources.exceptions.InternalServerErrorException;
import util.AtividadeBuilder;
import util.HBaseHelper;


public class UserAppHomeTimelineExample {

	private static final String COLUMN_FAMILY_INFO = "info";
	private static final String TABLE_NAME_ACTIVITIES = "atividades";
	private static final long USER = 1L;
	private static final long APP = 2L;
	private static final Map<Long, List<Long>> SEGUIDOS = new HashMap<Long, List<Long>>();
	private static Map<Long, Result> RESULTS = new TreeMap<Long, Result>();

	/**
	 * Implements the <code>Mapper</code> that reads the data and extracts the
	 * required information.
	 */
	// vv AnalyzeData
	static class AnalyzeMapper extends TableMapper<Text, IntWritable> { // co AnalyzeData-1-Mapper Extend the supplied TableMapper class, setting your own output key and value types.

		// ^^ AnalyzeData
		/**
		 * Maps the input.
		 *
		 * @param row The row key.
		 * @param columns The columns of the row.
		 * @param context The task context.
		 * @throws java.io.IOException When mapping the input fails.
		 */
		// vv AnalyzeData
		@Override
		public void map(ImmutableBytesWritable row, Result columns, Context context) throws IOException {
			String rowId = Bytes.toString(row.get());
			Long key = Long.valueOf(StringUtils.split(rowId, '-')[3]);

			RESULTS.put(key, columns);
		}
	}

	/**
	 * Main entry point.
	 *
	 * @param args  The command line parameters.
	 * @throws Exception When running the job fails.
	 */
	// vv AnalyzeData
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();

		clearDatabase(conf);

		populateHbase();

		populateFollowing();

		Scan scan = new Scan();
		List<Filter> filters = new ArrayList<Filter>();

		scan.addFamily(Bytes.toBytes(COLUMN_FAMILY_INFO));

		List<Long> seguidos = SEGUIDOS.get(USER);
		StringBuilder builder = new StringBuilder("^(");
		builder.append(StringUtils.leftPad(String.valueOf(USER), 20, '0'));

		for (Long seguido : seguidos) {
			String seguidoAsString = StringUtils.leftPad(String.valueOf(seguido), 20, '0');
			builder.append("|");
			builder.append(seguidoAsString);
		}
		builder.append(")-");
		builder.append(StringUtils.leftPad(String.valueOf(APP), 20, '0'));
		builder.append("-");
		builder.append(".+");

		filters.add(new RowFilter(CompareOp.EQUAL, new RegexStringComparator(builder.toString())));

		Job job = new Job(conf, "Analyze data in " + TABLE_NAME_ACTIVITIES);
		job.setJarByClass(UserAppHomeTimelineExample.class);
		TableMapReduceUtil.initTableMapperJob(TABLE_NAME_ACTIVITIES, scan, AnalyzeMapper.class, Text.class, IntWritable.class, job); // co AnalyzeData-6-Util Set up the table mapper phase using the supplied utility.
		job.setOutputFormatClass(NullOutputFormat.class);


		job.waitForCompletion(true);

		Set<Entry<Long, Result>> entrySet = RESULTS.entrySet();

		for (Entry<Long, Result> entry : entrySet) {
			Atividade atividade = HBaseRecordBuilder.builderFor(Atividade.class).toRecord(entry.getValue());
			System.out.println(entry.getKey() + ": " + atividade.getResultado().getFlexibleFields("corpo"));
		}

		System.exit(0);
	}

	private static void clearDatabase(Configuration conf) throws IOException {
		HBaseHelper helper = HBaseHelper.getHelper(conf);
		helper.dropTable(TABLE_NAME_ACTIVITIES);
		helper.createTable(TABLE_NAME_ACTIVITIES, COLUMN_FAMILY_INFO);
	}

	private static void populateFollowing() {
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
	}

	private static void populateHbase() throws InternalServerErrorException {
		AtividadeBuilder.anAtividade().withUsuario(1L).withApp(1L).withCorpo("Usuario 1; App1;").create();
		AtividadeBuilder.anAtividade().withUsuario(1L).withApp(2L).withCorpo("Usuario 1; App2;").create();

		AtividadeBuilder.anAtividade().withUsuario(1L).withApp(1L).withCorpo("Usuario 1; App1;").create();
		AtividadeBuilder.anAtividade().withUsuario(1L).withApp(2L).withCorpo("Usuario 1; App2;").create();

		AtividadeBuilder.anAtividade().withUsuario(1L).withApp(1L).withCorpo("Usuario 1; App1;").create();
		AtividadeBuilder.anAtividade().withUsuario(1L).withApp(2L).withCorpo("Usuario 1; App2;").create();

		AtividadeBuilder.anAtividade().withUsuario(3L).withApp(1L).withCorpo("Usuario 3; App1;").create();
		AtividadeBuilder.anAtividade().withUsuario(3L).withApp(2L).withCorpo("Usuario 3; App2;").create();

		AtividadeBuilder.anAtividade().withUsuario(2L).withApp(1L).withCorpo("Usuario 2; App1;").create();
		AtividadeBuilder.anAtividade().withUsuario(2L).withApp(2L).withCorpo("Usuario 2; App2;").create();

		AtividadeBuilder.anAtividade().withUsuario(4L).withApp(1L).withCorpo("Usuario 4; App1;").create();
		AtividadeBuilder.anAtividade().withUsuario(4L).withApp(2L).withCorpo("Usuario 4; App2;").create();

		AtividadeBuilder.anAtividade().withUsuario(1L).withApp(1L).withCorpo("Usuario 1; App1;").create();
		AtividadeBuilder.anAtividade().withUsuario(1L).withApp(2L).withCorpo("Usuario 1; App2;").create();
	}
}
