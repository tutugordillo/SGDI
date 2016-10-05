from mrjob.job import MRJob


class MRWordCount(MRJob):

	# Fase MAP (line es una cadena de texto)
  def mapper(self, key, line):
	L = line.split(",")
        yield L[2], L[8]

	# Fase REDUCE (key es una cadena texto, values un generador de valores)
  def reducer(self, key, values):
    copy = map(float,list(values))
    yield key, (min(copy), max(copy))

if __name__ == '__main__':
    MRWordCount.run()
