from mrjob.job import MRJob
import re
import os

class MRWordCount(MRJob):

	# Fase MAP (line es una cadena de texto)
  def mapper(self, key, line):
      for word in re.findall(r"[\w']+",line):
          yield word.lower(), os.environ['map_input_file']
	# Fase REDUCE (key es una cadena texto, values un generador de valores)

  def getKey(self,pair):
      return pair[1]

  def reducer(self, key, values):
      copy = list(values)
      files = set(copy)
      ret = []
      send = False 
      for e in files :
        times = copy.count(e)
        if(times > 20) :
          send = True
        ret = ret + [(e,times)]
      if send :
        ret = sorted(ret,key=self.getKey,reverse=True)
        yield key, ret

if __name__ == '__main__':
    MRWordCount.run()
