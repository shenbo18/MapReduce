package bo.kmeans;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
//import java.lang.reflect.Array;
import java.util.Arrays;

//import org.apache.commons.httpclient.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class KMeans extends Configured implements Tool {
   private static int numRows = 4601;
   private static int numCols = 58;
   private static int numClusters = 10;

   public static void main(String[] args) throws Exception {

      Configuration conf = new Configuration(); 
	  String[] paths = new String[2];
	  int res = -1;
	  
	  for (int i = 0; i< 20; i++) {
		  if (i == 0) {
			  conf.set("centroids", "c1.txt");  
		  } else {
			  String centroidsFile = paths[1] + "/" +"part-r-00000";
			  conf.set("centroids", centroidsFile);
		  }
		  
		  paths[0] = args[0];
		  paths[1] = args[1] + Integer.toString(i);
	      res = ToolRunner.run(conf, new KMeans(), paths);
	  }
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      System.out.println(Arrays.toString(args));
      
	  Configuration conf = getConf();	  
      Job job = new Job(conf, "KMeans");
      
//      Job job = new Job(getConf(), "KMeans");
      // add centroids file as distributed cache
//      DistributedCache.addCacheFile(new URI("/home/cloudera/workspace/KMeans/c1.txt"),getConf());
//      job.addCacheFile(new URI("/home/cloudera/workspace/KMeans/c1.txt"));
      job.setJarByClass(KMeans.class);
      
      job.setMapOutputKeyClass(IntWritable.class);
      job.setMapOutputValueClass(NewArrayWritable.class);
      
      job.setOutputKeyClass(NullWritable.class);
      job.setOutputValueClass(NewArrayWritable.class);

      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.waitForCompletion(true);
      
      return 0;
   }
   
   public static class Map extends Mapper<LongWritable, Text, IntWritable, NewArrayWritable> {
      private IntWritable cluster = new IntWritable();
      private IntWritable COST = new IntWritable(-1);
      private NewArrayWritable dataPoint = new NewArrayWritable();
      private float[][] centroids = new float[numClusters][numCols];
      
      public void setup(Context context) throws IOException {
    	  Configuration conf = context.getConfiguration();
    	  String centroidsFile = conf.get("centroids");
    	  
    	  // read centroids data
    	  File f = new File(centroidsFile);
    	  FileInputStream fis = null;
    	  try {
    		  fis = new FileInputStream(f);
    	  } catch (FileNotFoundException e) {
    		  // TODO Auto-generated catch block
    		  e.printStackTrace();
    	  }
    	  
    	  //Construct BufferedReader
    	  BufferedReader br = new BufferedReader(new InputStreamReader(fis));
    	  String line = null;

    	  int i = 0;
    	  while((line = br.readLine()) != null){
    		 int j = 0;
    		 for(String token: line.toString().split("\\s+")){
//        		 System.out.println(Integer.toString(i));
    			 centroids[i][j] = Float.parseFloat(token);
    			 j++;
    		 }

    		 i++;
    	  }
    	  br.close();
    	  
      }
      
      
      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {  	  
    	  FloatWritable[] point = new FloatWritable[numCols]; 
    	  FloatWritable[] cost = new FloatWritable[1];
    	  
    	  int j = 0;
    	  float[] sumDist = new float[numClusters];
    	  Arrays.fill(sumDist,0);
    	  
    	  for(String token: value.toString().split("\\s+")){
    		  float freq = Float.parseFloat(token);
    		  point[j] = new FloatWritable(freq);

    		  // Calculate contribution of this dimension to the distance to each centroids
    		  for (int k = 0; k< numClusters; k++){
    			  sumDist[k] = sumDist[k]+ (freq - centroids[k][j])*(freq - centroids[k][j]);
    		  }
    		  j++;
    	  }

    	  // Find the min distance from the data point to a centroid
    	  float minDist = Float.MAX_VALUE;
    	  int idx = -1;
    	  for (int k = 0; k< numClusters; k++) {
    		  if(minDist > sumDist[k]){
    			  minDist = sumDist[k];
    			  idx = k;
    		  }
    	  }
    	  // Output cluster assignment and data points as key-value pair
    	  cluster.set(idx);
    	  dataPoint.set(point);  
    	  context.write(cluster, dataPoint);
    	  cost[0] = new FloatWritable(minDist);
    	  dataPoint.set(cost);
    	  context.write(COST, dataPoint);
      }
   }

   public static class Reduce extends Reducer<IntWritable, NewArrayWritable, NullWritable, NewArrayWritable> {
	  private NewArrayWritable newCentroid = new NewArrayWritable();;
	   
      @Override
      public void reduce(IntWritable key, Iterable<NewArrayWritable> values, Context context)
              throws IOException, InterruptedException {
    	 // Determine the Cost of this iteration
    	 if (key.get() == -1) {
    		 float sum = 0;
    		 for (NewArrayWritable val: values) {
    			 FloatWritable[] cost = (FloatWritable[])val.toArray();
    			 sum += cost[0].get();
    		 }
    		 System.out.println(Float.toString(sum));
    		 return;
    	 }
    	  
    	 // Re-center cluster 
    	 int count = 0;
    	 float[] sum = new float[numCols];
    	 Arrays.fill(sum, 0);
    	 // Take the sum of all data points belong to the same cluster
    	 for (NewArrayWritable val: values){
    		 FloatWritable[] point = (FloatWritable[])val.toArray();
    		 for (int i = 0; i < numCols; i++){
    			 sum[i] = sum[i] + point[i].get();
    		 }
    		 count++;
    	 }
    	 
    	 // Take the average to find the new centroid
    	 FloatWritable[] average = new FloatWritable[numCols];
    	 for (int j= 0; j< numCols; j++){
    		 average[j] = new FloatWritable(sum[j]/count);
    	 }
    	 newCentroid.set(average);
    	 context.write(NullWritable.get(), newCentroid);
      }
   }
   
   public static class NewArrayWritable extends ArrayWritable {
	   public NewArrayWritable() {
		   super(FloatWritable.class);
	   }
	   @Override
	   public String toString() {
		   StringBuilder sb = new StringBuilder();
		   for (FloatWritable f: (FloatWritable[])toArray()) {
			   sb.append(Float.toString(f.get()));
			   sb.append(" ");
		   }
		   return sb.toString();
	   }
   }
}
