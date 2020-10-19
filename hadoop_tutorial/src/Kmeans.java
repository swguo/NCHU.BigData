import java.io.IOException;
import java.util.ArrayList;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
 
import java.util.Arrays;
import java.util.*;
 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.LineReader;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;



public class Kmeans {
    
    public static class Map extends Mapper<LongWritable, Text, IntWritable, Text>{
 
        //center cluster
        ArrayList<ArrayList<Text>> centers = null;
        //number of k
        int k = 0;
        
        
        protected void setup(Context context) throws IOException,
                InterruptedException {
            System.out.print("read centers");
            //從檔案抓出中心點
            centers = getCentersFromHDFS(context.getConfiguration().get("centersPath"),false);
            System.out.print("number of k");
            k = centers.size();
        }
 
 
       
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            
            //來源檔中所有大里PM2.5數據
            ArrayList<Text> fileds = textToArray(value);
            //共有幾筆PM2.5資料
            int sizeOfFileds = fileds.size();
            
            double minDistance = Double.MAX_VALUE;
            int centerIndex = 0;
            
            //分k群
            for(int i=0;i<k;i++){
                double currentDistance = 0;

                //每天PM2.5數據與4個中心點計算距離
                for(int j=3;j<sizeOfFileds;j++){
                    double centerPoint = Math.abs(Double.parseDouble(centers.get(i).get(j).toString()));
                    double filed = Math.abs(Double.parseDouble(fileds.get(j).toString()));
                    currentDistance += Math.pow(centerPoint - filed, 2);
                }

                //找到最靠近第k個中心點指標
                if(currentDistance<minDistance){
                    minDistance = currentDistance;
                    centerIndex = i;
                }
            }
            //將同一群的PM2.5數據放在同一個MAP
            context.write(new IntWritable(centerIndex+1), value);
        }
        
    }
    
    
    public static class Reduce extends Reducer<IntWritable, Text, Text, Text>{
 
        
        protected void reduce(IntWritable key, Iterable<Text> value,Context context)
                throws IOException, InterruptedException {
            ArrayList<ArrayList<Text>> filedsList = new ArrayList<ArrayList<Text>>();
            
            //各點PM2.5轉存成
            for(Iterator<Text> it =value.iterator();it.hasNext();){
               
                ArrayList<Text> tempList = textToArray(it.next());
                filedsList.add(tempList);
            }
            
            //calculate new center
            //each row number of elenmt
            int filedSize = filedsList.get(0).size();
            double[] avg = new double[filedSize-3];
            for(int i=3;i<filedSize;i++){ //0~23 hr
                //caculate average
                double sum = 0;
                int size = filedsList.size(); //每個分類幾筆數據
                for(int j=0;j<size;j++){
                    sum += Double.parseDouble(filedsList.get(j).get(i).toString());
                }
                
                avg[i-3] = sum / size;
            }

            //找出最靠近中心點的日期
            double minDistance = Double.MAX_VALUE;
            int index_min = 0; 
            int size = filedsList.size();
            for(int i=0;i<size;i++){ //每個分類幾筆數據                            
                double currentDistance = 0;                
                for(int j=3;j<filedSize;j++){ //0~23 hr
                    double p = Math.abs(avg[j-3]); //中心點平均
                    double q = Math.abs(Double.parseDouble(filedsList.get(i).get(j).toString()));
                    currentDistance += Math.pow(p - q, 2); // 歐基里德距離公式
                }
                if(currentDistance<minDistance){
                    index_min = i;
                }
            }

            String date = filedsList.get(index_min).get(0).toString();
            context.write(new Text("") , new Text(date+",大里"+",PM25,"+Arrays.toString(avg).replace("[", "").replace("]", "")));
        }
        
    }
    
    @SuppressWarnings("deprecation")
    public static void run(String centerPath,String dataPath,String newCenterPath,boolean runReduce) throws IOException, ClassNotFoundException, InterruptedException{
        
        Configuration conf = new Configuration();
        conf.set("centersPath", centerPath);
        
        Job job = new Job(conf, "K-means");
        job.setJarByClass(Kmeans.class);
        
        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        if(runReduce){
            //最後一次不執行Reduce
            job.setReducerClass(Reduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
        }
        
        FileInputFormat.addInputPath(job, new Path(dataPath));
        
        FileOutputFormat.setOutputPath(job, new Path(newCenterPath));
        job.waitForCompletion(true);
        System.out.println("");
    }
    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
    	String localhost = "master";
        String centerPath = args[1];//"hdfs://"+localhost+":9000/user/swguo/cluster.center.conf.txt"; //
        String dataPath = args[0];//"hdfs://"+localhost+":9000/user/swguo/kmeans.txt"; //
        String newCenterPath = args[2]; // "hdfs://"+localhost+":9000/user/swguo/kmeans";//
        
        int count = 0;
        
        
        while(true){
            
            run(centerPath,dataPath,newCenterPath,true);
            System.out.println(" epoch " + ++count);
            if(compareCenters(centerPath,newCenterPath )){
                run(centerPath,dataPath,newCenterPath,false);
                //break;
            }
            
            if(count >= Integer.parseInt(args[3])){ // 執行幾次
                run(centerPath,dataPath,newCenterPath,false);
                break;
            }
        }
    }
    //從HDFS抓各群集中心點
    public static ArrayList<ArrayList<Text>> getCentersFromHDFS(String centersPath,boolean isDirectory) throws IOException{
        
        ArrayList<ArrayList<Text>> result = new ArrayList<ArrayList<Text>>();
        
        Path path = new Path(centersPath);
        
        Configuration conf = new Configuration();
        
        FileSystem fileSystem = path.getFileSystem(conf);
 
        if(isDirectory){    
            FileStatus[] listFile = fileSystem.listStatus(path);
            for (int i = 0; i < listFile.length; i++) {
                result.addAll(getCentersFromHDFS(listFile[i].getPath().toString(),false));
            }
            return result;
        }
        
        FSDataInputStream fsis = fileSystem.open(path);
        LineReader lineReader = new LineReader(fsis, conf);
        
        Text line = new Text();
        
        while(lineReader.readLine(line) > 0){
            ArrayList<Text> tempList = textToArray(line);
            //53,55,58,...,16 to [53,55,58,...,16]
            result.add(tempList);
        }
        lineReader.close();
        return result;
    }
    
   
    public static void deletePath(String pathStr) throws IOException{
        Configuration conf = new Configuration();
        Path path = new Path(pathStr);
        FileSystem hdfs = path.getFileSystem(conf);
        hdfs.delete(path ,true);
    }
    
    public static ArrayList<Text> textToArray(Text text){ 
        ArrayList<Text> list = new ArrayList<Text>();
        String[] fileds = text.toString().split(",");
        /*for(String val:fileds){
        	System.out.println(val);
        }*/
        for(int i=0;i<fileds.length;i++){
            list.add(new Text(fileds[i]));
        }
        return list;
    }
    
    public static boolean compareCenters(String centerPath,String newPath) throws IOException{
        
        List<ArrayList<Text>> oldCenters = getCentersFromHDFS(centerPath,false);
        List<ArrayList<Text>> newCenters = getCentersFromHDFS(newPath,true);
        
        int size = oldCenters.size();
        int fildSize = oldCenters.get(0).size();
        double distance = 0;
        for(int i= 0;i<size;i++){
            for(int j=3;j<fildSize;j++){
                double p = Math.abs(Double.parseDouble(oldCenters.get(i).get(j).toString()));
                double q = Math.abs(Double.parseDouble(newCenters.get(i).get(j).toString()));
                distance += Math.pow(p - q, 2); // 歐基里德距離公式
            }
        }
        
        if(distance == 0.0){            
            deletePath(newPath);
            return true;
        }else{           
            
            Configuration conf = new Configuration();
            Path outPath = new Path(centerPath);
            FileSystem fileSystem = outPath.getFileSystem(conf);
            
            FSDataOutputStream overWrite = fileSystem.create(outPath,true);
            overWrite.writeChars("");
            overWrite.close();
            
            
            Path inPath = new Path(newPath);
            FileStatus[] listFiles = fileSystem.listStatus(inPath);
            for (int i = 0; i < listFiles.length; i++) {                
                FSDataOutputStream out = fileSystem.create(outPath);
                FSDataInputStream in = fileSystem.open(listFiles[i].getPath());
                IOUtils.copyBytes(in, out, 4096, true);
            }
            
            deletePath(newPath);
        }
        
        return false;
    }
    
}



