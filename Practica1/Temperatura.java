/**
 * Adaptación del ejemplo original en http://wiki.apache.org/hadoop/WordCount
 */

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * <p>Este ejemplo cuenta el número de veces que aparece cada palabra en el 
 * archivo de entrada usando MapReduce. El código tiene 3 partes: mapper, 
 * reduce y programa principal</p>
 */
public class Temperatura {

  /**
   * <p>
   * El mapper extiende de la interfaz org.apache.hadoop.mapreduce.Mapper. Cuando
   * se ejecuta Hadoop el mapper recibe cada linea del archivo de entrada como
   * argumento. La función "map" parte cada línea y para cada palabra emite la
   * pareja (word,1) como salida.</p>
   */
  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, DoubleWritable>{
    
    private DoubleWritable temp = new DoubleWritable();
    private Text day = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
          
        String  line = value.toString();
        String[] tokens = line.split(",");
	day.set(tokens[2]);
	temp.set(Double.parseDouble(tokens[8]));
        //one.set(Integer.parseInt(tokens[8]));
        context.write(day,temp);
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
       extends Reducer<Text,DoubleWritable,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<DoubleWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      double min = Double.MAX_VALUE;
      double max = Double.MIN_VALUE;
      for (DoubleWritable val : values) {
        double temp = val.get();
	if(temp < min) min = temp;
	if(temp > max) max = temp; 
      }
      String minS = String.valueOf(min);
      String maxS = String.valueOf(max);
      String temperature = "("+minS+", "+maxS+")";
      result.set(temperature);
      context.write(key, result);
    }
  }

  /**
   * <p>Clase principal con método main que iniciará la ejecución de la tarea</p>
   */
  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf();
    Job job = Job.getInstance(conf);
    job.setJarByClass(Temperatura.class);
    job.setMapperClass(TokenizerMapper.class);
    //Si existe combinador
    //job.setCombinerClass(Clase_del_combinador.class);
    job.setReducerClass(IntSumReducer.class);

    // Declaración de tipos de salida para el mapper
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DoubleWritable.class);
    // Declaración de tipos de salida para el reducer
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // Archivos de entrada y directorio de salida
    FileInputFormat.addInputPath(job, new Path( "JCMB_last31days.csv" ));
    FileOutputFormat.setOutputPath(job, new Path( "salida" ));
    
    // Aquí podemos elegir el numero de nodos Reduce
    // Dejamos 1 para que toda la salida se guarde en el mismo fichero 'part-r-00000'
    job.setNumReduceTasks(1);

		// Ejecuta la tarea y espera a que termine. El argumento boolean es para 
    // indicar si se quiere información sobre de progreso (verbosity)
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
