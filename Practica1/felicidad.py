from mrjob.job import MRJob


class MRWordCount(MRJob):

	# Fase MAP (line es una cadena de texto)
  def mapper(self, key, line):
	L = line.split('\t')
        if float(L[2]) < 2 and L[4] != "--":
		yield "sad", L[0]
	# Fase REDUCE (key es una cadena texto, values un generador de valores)
  def reducer(self, key, values):
    copy = list(values)
    yield  len(copy), copy

if __name__ == '__main__':
    MRWordCount.run()
