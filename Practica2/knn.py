
from scipy import spatial as sp
import sys
import operator 

'''
Auxiliary function that process a line given. It eliminates the special character \n and split the line in its components (they are separated by , ). After that the method convert the strings with numerical values in float.

Arguments: 
          line -- string that contains the values of each attributes of one instance.

Returns:
          elements -- list which size is equals to the number of attributes of the instances and contains the attributes of the instances processed.
'''
def process_line(line):
    
    line = line.rstrip('\n');
    elements_aux = line.split(",");
    elements = map(float,elements_aux[:-1]);
    elements.append(elements_aux[-1]);
    return elements

'''
Function that reads the lines of a given file, processes each line and returns a list with the processed data.

Arguments:
           filename -- string that identifies the file to process.


Returns: 
           A list whose lenght are the same as the lines processed and that contains one list for each line processed. These lists contains the attributes of each instance.
'''
def read_file(filename):
    file = open(filename,'r');
    lines = file.readlines();
    lines = lines[1::];
    return map(process_line,lines);

'''
Function that given a numerical value k, an instance i and a trainning set c, returns the k near neighbors to i of the set c. It checks if the instances to classify has a class or not. 

Arguments:
           k -- numerical value that represents the number of nearest neighbors I want to consider in the algorithm.
           i -- list that contains the values of the instances to classify.
           c -- list that contains lists. Each lists represents a instance of the trainning set.

Returns:
           A list that contains pairs with the distances of the instance i and the class of the k nearest neighbours.
'''

def kNeighbors(k,i,c):
    neig=[];
    distances = [];
    
    if (len(i) == len(c[1])):
        for c_aux in c :
            distances.append((sp.distance.euclidean(i[:-1],c_aux[:-1]),c_aux[-1]));
        
    else :
        for x in xrange (len(c)):
            distances.append((sp.distance.euclidean(i,c[x][:-1]),x));

    distances.sort();
   
    return distances[:k];
            
'''
Given a value k, a new instance i and a trainning set c it returns the class in which the instances i will be classify.

Arguments:
           k -- integer that identifies the k nearest neighbors.
           i -- list that represents the new instance to be classified.
           c -- list that contains lists and represents the trainning set. Each of its list represents one instances of the trainning set.

Returns: 
           It returns the class in which the instances i has been classified or -1 if an error happens.
'''
def knn(k,i,c): 
    
   # distances = {"euclidean":"euclidean","manhattan":"cityblock"}
    
    if (len(i) == len(c[1])) or (len(i) == len(c[1])-1) :
        nSet = kNeighbors(k,i,c);
        moda={};
        
        for a in nSet :
            cl = a[1];
            if cl in moda :
                moda[cl]+=1;
            else:
                moda[cl]=1;
                
        # We use iteritems to convert the dicctionary moda in a iterator.
        # We use itemgetter to obtain the second value of the tuple.
        return max(moda.iteritems(), key=operator.itemgetter(1))[0];
        
    else :
        print "Error. La instancia no tiene los mismos atributos que las instancias existentes en el conjunto de entrenamiento";
        return -1


'''
Function that given a trainning set and a test set, classifies all the instances of the test set using the k-nn algorithm and calculates the procentage of them that has been correctly classified.

Arguments:
          k --
          trainset --
          testset --

Returns:
          The procentage of instances that have been correctly classified.
'''
def test(k,trainset,testset):
    
    result = [knn(k,x, trainset) for x in testset];
    cont = 0;
    print result

    for i in xrange (len(testset)):
        if(result[i]==testset[i][-1]):
            cont = cont +1;
    
    return (cont*1.0)/len(testset);
    

if __name__ == "__main__":
    k = 60
    trainSet = read_file(sys.argv[1]);
    testSet = read_file(sys.argv[2]);
    print test(k,trainSet,testSet);
