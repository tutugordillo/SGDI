/**
 * Adaptación del ejemplo original en http://wiki.apache.org/hadoop/WordCount
 */

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;
import java.util.ArrayList;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * <p>Este ejemplo cuenta el número de veces que aparece cada palabra en el 
 * archivo de entrada usando MapReduce. El código tiene 3 partes: mapper, 
v * reduce y programa principal</p>
 */
public class Indice_invertido {

  /**
   * <p>
   * El mapper extiende de la interfaz org.apache.hadoop.mapreduce.Mapper. Cuando
   * se ejecuta Hadoop el mapper recibe cada linea del archivo de entrada como
   * argumento. La función "map" parte cada línea y para cada palabra emite la
   * pareja (word,1) como salida.</p>
   */
  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, Text>{
    
    private Text book = new Text();
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
          
        String  line = value.toString();
        String[] tokens = line.split("[\\W]");
        String file = ((FileSplit)context.getInputSplit()).getPath().getName();
	for(String token : tokens){
            word.set(token.toLowerCase());
            book.set(file);
	    context.write(word,book);
	}
    }
  }
  




    public static class Pair implements Comparable<Pair>{
	private int count;
	private String book;

	public Pair(){
	    this.count = 0;
	    this.book = "";
	}

	public Pair(int count, String book){
	    this.count = count;
	    this.book = book;
	}

	public int getCount(){
	    return this.count;
	}


	public int compareTo(Pair o){
	    if (this.count<o.getCount()) return 1;
	    else if(this.count == o.getCount()) return 0;
	    else return -1;
	}

	public String toString(){
	    return "("+this.book+","+String.valueOf(this.count)+")";
	}
	
    }





  /**
   * <p>La función "reduce" recibe los valores (apariciones) asociados a la misma
   * clave (palabra) como entrada y produce una pareja con la palabra y el número
   * total de apariciones. 
   * Ojo: como las parejas generadas por la función "map" siempre tienen como 
   * valor 1 se podría evitar la suma y devolver directamente el número de 
   * elementos.</p>  
   */
  public static class IntSumReducer 
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      ArrayList<String> vals = new ArrayList<String>();
      for(Text e : values){
	  vals.add(e.toString());
      }
      HashSet<String> files = new HashSet<String>();
      files.addAll(vals);
      boolean send = false;
      String occurrences = "";
      ArrayList<Pair> list = new ArrayList<Pair>();
      //int timesOld = Integer.MIN_VALUE;
      for(String e : files){
	  int times = Collections.frequency(vals,e);
	  if(times > 20) send = true;
	  list.add(new Pair(times,e));
	  //if(times>timesOld){
	  //    occurrences = "("+e+","+times+")"+occurrences;
	  //}else{
	  //    occurrences += "("+e+","+times+")";}
	  //timesOld = times;
      }
      Collections.sort(list);
      
      for(Pair a :list){
	  occurrences = occurrences+a.toString(); 
      }
      result.set(occurrences);
      if(send) context.write(key, result);
    }
  }

  /**
   * <p>Clase principal con método main que iniciará la ejecución de la tarea</p>
   */
  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf();
    Job job = Job.getInstance(conf);
    job.setJarByClass(Indice_invertido.class);
    job.setMapperClass(TokenizerMapper.class);
    //Si existe combinador
    //job.setCombinerClass(Clase_del_combinador.class);
    job.setReducerClass(IntSumReducer.class);

    // Declaración de tipos de salida para el mapper
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    // Declaración de tipos de salida para el reducer
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // Archivos de entrada y directorio de salida
    FileInputFormat.addInputPath(job, new Path( "Hamlet.txt" ));
    FileInputFormat.addInputPath(job, new Path( "Moby_Dick.txt" ));
    FileInputFormat.addInputPath(job, new Path( "Adventures_of_Huckleberry_Finn.txt" ));
    FileOutputFormat.setOutputPath(job, new Path( "salida" ));
    
    // Aquí podemos elegir el numero de nodos Reduce
    // Dejamos 1 para que toda la salida se guarde en el mismo fichero 'part-r-00000'
    job.setNumReduceTasks(1);

		// Ejecuta la tarea y espera a que termine. El argumento boolean es para 
    // indicar si se quiere información sobre de progreso (verbosity)
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
