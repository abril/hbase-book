package mapreduce;

// cc AnalyzeData MapReduce job that reads the imported data and analyzes it.
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import socialcore.resources.exceptions.InternalServerErrorException;
import util.AtividadeBuilder;
import util.HBaseHelper;


public class AnalyzeData {

	private static final String COLUMN_FAMILY_INFO = "info";
	private static final String TABLE_NAME_ACTIVITIES = "atividades";

	private static final Log LOG = LogFactory.getLog(AnalyzeData.class);

	public static final String NAME = "AnalyzeData";
	public enum Counters { ROWS, COLS, ERROR, VALID }
	
	private static final long USER = 1L;
	private static final long APP = 2L;
	private static final Map<Long, List<Long>> SEGUIDOS = new HashMap<Long, List<Long>>();
	
	/**
	 * Implements the <code>Mapper</code> that reads the data and extracts the
	 * required information.
	 */
	// vv AnalyzeData
	static class AnalyzeMapper extends TableMapper<Text, IntWritable> { // co AnalyzeData-1-Mapper Extend the supplied TableMapper class, setting your own output key and value types.

		private IntWritable ONE = new IntWritable(1);
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
			System.out.println("AnalyzeMapper.map");
			System.out.println(Bytes.toString(row.get()));

			context.getCounter(Counters.ROWS).increment(1);
			String value = null;
			try {
				for (KeyValue kv : columns.list()) {
					context.getCounter(Counters.COLS).increment(1);
					value = Bytes.toStringBinary(kv.getValue());
					// ^^ AnalyzeData
					// vv AnalyzeData
					context.write(new Text("chupaki"), ONE);
					context.getCounter(Counters.VALID).increment(1);
				}
			} catch (Exception e) {
				e.printStackTrace();
				System.err.println("Row: " + Bytes.toStringBinary(row.get()) +
						", JSON: " + value);
				context.getCounter(Counters.ERROR).increment(1);
			}
		}
		// ^^ AnalyzeData
		/*
       {
         "updated": "Mon, 14 Sep 2009 17:09:02 +0000",
         "links": [{
           "href": "http://www.webdesigndev.com/",
           "type": "text/html",
           "rel": "alternate"
         }],
         "title": "Web Design Tutorials | Creating a Website | Learn Adobe
             Flash, Photoshop and Dreamweaver",
         "author": "outernationalist",
         "comments": "http://delicious.com/url/e104984ea5f37cf8ae70451a619c9ac0",
         "guidislink": false,
         "title_detail": {
           "base": "http://feeds.delicious.com/v2/rss/recent?min=1&count=100",
           "type": "text/plain",
           "language": null,
           "value": "Web Design Tutorials | Creating a Website | Learn Adobe
               Flash, Photoshop and Dreamweaver"
         },
         "link": "http://www.webdesigndev.com/",
         "source": {},
         "wfw_commentrss": "http://feeds.delicious.com/v2/rss/url/
             e104984ea5f37cf8ae70451a619c9ac0",
         "id": "http://delicious.com/url/
             e104984ea5f37cf8ae70451a619c9ac0#outernationalist"
       }
		 */
		// vv AnalyzeData
	}

	// ^^ AnalyzeData
	/**
	 * Implements the <code>Reducer</code> part of the process.
	 */
	// vv AnalyzeData
	static class AnalyzeReducer extends Reducer<Text, IntWritable, Text, IntWritable> { // co AnalyzeData-3-Reducer Extend a Hadoop Reducer class, assigning the proper types.

		// ^^ AnalyzeData
		/**
		 * Aggregates the counts.
		 *
		 * @param key The author.
		 * @param values The counts for the author.
		 * @param context The current task context.
		 * @throws IOException When reading or writing the data fails.
		 * @throws InterruptedException When the task is aborted.
		 */
		// vv AnalyzeData
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			System.out.println("AnalyzeReducer.reduce");
			System.out.println(Bytes.toString(key.getBytes()));
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
		/*...*/
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
		populateHbase();
		
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


		// vv AnalyzeData
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

		Job job = new Job(conf, "Analyze data in " + TABLE_NAME_ACTIVITIES);
		job.setJarByClass(AnalyzeData.class);
		TableMapReduceUtil.initTableMapperJob(TABLE_NAME_ACTIVITIES, scan, AnalyzeMapper.class, Text.class, IntWritable.class, job); // co AnalyzeData-6-Util Set up the table mapper phase using the supplied utility.
		job.setReducerClass(AnalyzeReducer.class);
		job.setOutputKeyClass(Text.class); // co AnalyzeData-7-Output Configure the reduce phase using the normal Hadoop syntax.
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(1);
		FileUtil.fullyDelete(new File("/tmp/pegaNaMinha"));
		FileOutputFormat.setOutputPath(job, new Path("/tmp/pegaNaMinha"));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	// ^^ AnalyzeData



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
