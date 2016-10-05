/**
 * Adaptación del ejemplo original en http://wiki.apache.org/hadoop/WordCount
 */

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.IntWritable;
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
public class ServidorWeb {

    public static class LogServerWritable implements Writable {

    public LogServerWritable(){
	this.pet = 0;
	this.code = 0;
	this.size = 0;
    }
    
   
    public LogServerWritable(int pet, int code, double size){
	this.pet = pet;
	this.code = code;
	this.size = size;
    }

    public LogServerWritable(String pet, String code, String size){
	this.pet = Integer.parseInt(pet);
	this.code = Integer.parseInt(code);
	if(size.equals("-")){
		this.size = 0;}
	    else{ this.size = Double.parseDouble(size);} 
    }

    public void setPet(int pet){
	this.pet = pet;
    }
    
    public void setCode(int code){
	this.code = code;
    }

    public void setSize(double size){
	this.size = size;
    }

    public int getPet(){
	return this.pet;
    }

    public int getCode(){
	return this.code;
    }

    public double getSize(){
	return this.size;
    }

    public void write(DataOutput dataOutput) throws IOException {
	dataOutput.writeInt(this.pet);
	dataOutput.writeInt(this.code);
	dataOutput.writeDouble(this.size);
    }

    public void readFields(DataInput dataInput) throws IOException {
	this.pet = dataInput.readInt();
	this.code = dataInput.readInt();
	this.size = dataInput.readDouble();
    }
    

    private int pet;
    private int  code;
    private double size;
}

  /**
   * <p>
   * El mapper extiende de la interfaz org.apache.hadoop.mapreduce.Mapper. Cuando
   * se ejecuta Hadoop el mapper recibe cada linea del archivo de entrada como
   * argumento. La función "map" parte cada línea y para cada palabra emite la
   * pareja (word,1) como salida.</p>
   */
  public static class TokenizerMapper 
      extends Mapper<Object, Text,Text,LogServerWritable>{
    
    private final static Text clave = new Text();
    private  static LogServerWritable attr;
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
          
        String  line = value.toString();
        String[] tokens = line.split(" ");
	int len = tokens.length;
	clave.set(tokens[0]);
	attr = new LogServerWritable("1", tokens[len-2], tokens[len-1]);
	context.write(clave,attr);

	}
	
  }

/**
CLASE COMBINER




 */

public static class Combiner 
      extends Reducer<Text,LogServerWritable,Text,LogServerWritable> {
    private static LogServerWritable result;
   
    public void reduce(Text key, Iterable<LogServerWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      
 
	int pet = 0;
	double tam = 0;
	int err = 0;
	for (LogServerWritable val : values) {
	    pet = pet+val.getPet();
	    tam = tam+val.getSize();
	    if(val.getCode()>=400 && val.getCode()<600){
	      err++;
	  }
	  
	 
      }
	result = new LogServerWritable(pet,err,tam);
	context.write(key, result);
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
      extends Reducer<Text,LogServerWritable,Text,Text> {
    private Text result = new Text();
   
    public void reduce(Text key, Iterable<LogServerWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      
        
	int pet = 0;
	double tam = 0;
	int err = 0;
      for (LogServerWritable val : values) {
	  pet = pet+val.getPet();
	  tam = tam+val.getSize();
	  err = err+val.getCode();
      }
      String cadena = "("+String.valueOf(pet)+", "+String.valueOf(tam)+", "+String.valueOf(err)+")";
      
      result.set(cadena);
      
      context.write(key, result);
    }
  }


  /**
   * <p>Clase principal con método main que iniciará la ejecución de la tarea</p>
   */
  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf();
    Job job = Job.getInstance(conf);
    job.setJarByClass(ServidorWeb.class);
    job.setMapperClass(TokenizerMapper.class);
    //Si existe combinador
    job.setCombinerClass(Combiner.class);
    job.setReducerClass(IntSumReducer.class);

    // Declaración de tipos de salida para el mapper
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LogServerWritable.class);
    // Declaración de tipos de salida para el reducer
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // Archivos de entrada y directorio de salida
    FileInputFormat.addInputPath(job, new Path( "weblog.txt" ));
    FileOutputFormat.setOutputPath(job, new Path( "salida" ));
    
    // Aquí podemos elegir el numero de nodos Reduce
    // Dejamos 1 para que toda la salida se guarde en el mismo fichero 'part-r-00000'
    job.setNumReduceTasks(1);

		// Ejecuta la tarea y espera a que termine. El argumento boolean es para 
    // indicar si se quiere información sobre de progreso (verbosity)
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
