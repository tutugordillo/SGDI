from scipy import spatial as sp
from mrjob.job import MRJob
import numpy as np
import sys
import operator 


class KnnMapRed(MRJob):
  
  count = 0;
  test_file = "/home/pablo/Descargas/iris_test.csv";
  
  def process_line(self,line):
    line = line.rstrip('\n');
    elements_aux = line.split(",");
    elements = map(float,elements_aux[:-1]);
    elements.append(elements_aux[-1]);
    return elements

  def calcular_dist(self,a,b,dist):
    
      if dist == "euclidean" :
          result = sp.distance.euclidean(a,b)
      elif dist == "manhattan" :
          result = sp.distance.cityblock(a,b)
    
      return result
  
  def calcular_moda(self, L):
      dict_m = {}
      for x in L :
          clase = x[1]
          if x[1] in dict_m:
              dict_m[clase]+=1
          else:
              dict_m[clase] = 1
        
      return dict_m
  
  def mapper_init(self):
      file = open(self.test_file,'r');
      lines = file.readlines();
      lines = lines[1::];
      self.test_set = map(self.process_line,lines);
      

	# Fase MAP (line es una cadena de texto con cada instancia del conjunto de entrenamiento)
  def mapper(self, key, line):
      
      if self.count >0 :
        L = line.split(",")
        clase = L[-1]
        inst = map(float,L[:-1])
        inst.append(clase)
        for x in xrange(len(self.test_set)):
          dist = self.calcular_dist(self.test_set[x][:-1],inst[:-1],"euclidean")
          yield x, (dist,clase)

      self.count = self.count +1


  def reducer_init(self):
    self.k = 60

# La fase REDUCE recibe una lista con las distancias y la clase asociada a cada una y se quedara con las k mas grandes.
  def reducer(self, key, values):
   
    copy = list(values)
    copy.sort()
    k_neigh = copy[:self.k]
    moda = self.calcular_moda(k_neigh)
    max_element = max(moda.iteritems(), key=operator.itemgetter(1))
    yield  key, max_element[0]



if __name__ == "__main__":
    KnnMapRed.run()
