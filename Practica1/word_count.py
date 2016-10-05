from mrjob.job import MRJob


class MRWordCount(MRJob):

	# Fase MAP (line es una cadena de texto)
  def mapper(self, key, line):
    for word in line.split():
      yield word, 1

	# Fase REDUCE (key es una cadena texto, values un generador de valores)
  def reducer(self, key, values):
    yield key, sum(values)


if __name__ == '__main__':
    MRWordCount.run()
