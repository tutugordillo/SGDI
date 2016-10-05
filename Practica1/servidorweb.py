from mrjob.job import MRJob
import re

class MRWordCount(MRJob):

	# Fase MAP (line es una cadena de texto)
  def mapper(self, key, line):
      L = line.split();
      length = len(L)
      host = L[0]
      codigoHTTP = L[length-2]
      nBytes = L[length-1]
      yield host, (nBytes,codigoHTTP)



  def combiner(self, key, values):
    cont = 0
    tamTotal = 0
    errores = 0
    for (nByte,codigo) in values :
      cont += 1
      if nByte != "-":
        tamTotal += float(nByte)
      if codigo.startswith('4') or codigo.startswith('5'):
        errores += 1
        
    result = (cont,tamTotal,errores)
    yield key, result



	# Fase REDUCE (key es una cadena texto, values un generador de valores)

  def reducer(self, key, values) :
    cont = 0
    tamTotal = 0
    errores = 0
    for (pet,nByte,codigo) in values :
      cont = cont+pet
      errores = errores+codigo
      tamTotal = tamTotal+nByte
        
    result = (cont,tamTotal,errores)
    yield key, result

if __name__ == '__main__':
    MRWordCount.run()
