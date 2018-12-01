import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HashtagCounterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	//private final static Pattern SPLIT_PATTER = Pattern.compile("(\\s*\\b\\s*)");
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

	  String line = value.toString();
	   
	  Text currentWord=new Text();
	  String words[] = line.split(" ");// dividimos todas las lineas 
	  for (int i=0;i<words.length;i++){
		  if(words[i].isEmpty()){
			  continue;
		  }
		  if((words[i].startsWith("#"))){// Si la palabra empieza por #
		  words[i] =words[i].replaceAll("[^a-zA-Z0-9]", ""); //Estandarizamos los hastag
		  currentWord = new Text(words[i]); // comvertimos el formato de la palabra
		  context.write(currentWord, one);// realizamos la tupla de palabras 
		  }
	  }
  }
}
