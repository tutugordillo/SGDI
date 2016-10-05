from scipy import spatial as sp
import numpy as np
import sys
import math
import operator
import matplotlib.pyplot as plt

'''
Auxiliary function that process each line
'''
def process_line( line):
    line = line.rstrip('\n');
    line = line.rstrip('\r');
    elements = line.split(",");
    elements = map(float,elements);
    return elements

'''
Function that reads the lines of a file given, processes each line and returns a list with the processed data of these.

Arguments:
           filename -- string that identifies the file to process.


Returns: 
           A list whose lenght are the same as the lines processed and that contains one list for each line processed with their dates.
'''
def read_file(filename):
    file = open(filename,'r');
    lines = file.readlines();
    lines = lines[1::];
    return map(process_line,lines);


'''

'''
def init_centroides(k,instancias):
    centroides = [instancias[0]];
    dist_conj = [];
    for i in xrange(k-1):
        dist_al_conjunto = []
        for inst in instancias :
            dist_a_centroides = [];
            for centroide in centroides :
                dist_a_centroides.append((sp.distance.euclidean(inst,centroide),inst));
            dist_a_centroides.sort();
            dist_al_conjunto.append(dist_a_centroides[0]);
        dist_al_conjunto.sort();
        (dist,centroide) = dist_al_conjunto[-1];
        centroides.append(centroide);
    return centroides;
                

def calcularCentroide(cluster):
    centroide = np.asarray([0.0 for i in xrange(len(cluster[0]))]);
    for x in cluster :       
        centroide += np.asarray(x);
    centroide *= 1.0/len(cluster);
    return centroide.tolist();

def kmeans(k, instancias, centroides_ini = None):
    if centroides_ini == None :
         centroides_ini = init_centroides(k,instancias);
    cluster = {} 
    for ind in xrange(k) :
         cluster[ind] = [];
### Calculamos la nueva particion de las instancias en los k clusters.
    i = 0;
    for inst in instancias :
        distancias = [];
        for ind in xrange(k) :
               distancias.append((sp.distance.euclidean(inst,centroides_ini[ind]),ind));
        distancias.sort();
        (minDist,ind) = distancias[0];
        cluster_parcial = cluster[ind];
        cluster_parcial += ([inst]);
        cluster[ind] = cluster_parcial;
        i = i + 1;
### Calculamos el nuevo centroide de cada cluster.
    centroides_end = [0.0 for i in xrange(k)]
    for x in xrange(k) :
       clusterX = cluster[x];
       centroides_end[x] = calcularCentroide(clusterX);
### Comprobamos si alcanzamos el punto fijo.
    dist = np.asarray(centroides_ini) - np.asarray(centroides_end);
    if np.linalg.norm(dist) > 0 :
        return kmeans(k,instancias,centroides_end);
    else :
        return (cluster,centroides_ini);

def calculaRadio(cluster,centroide) :
    distancias = [];
    for inst in cluster :
        distancias.append(sp.distance.euclidean(inst,centroide));
    distancias.sort();
    return distancias[-1];

def calculaDiametro(cluster) :
    dists = np.zeros((len(cluster),len(cluster)));
    for i in xrange(len(cluster)) :
        for j in xrange(i,len(cluster)) :
            dists[i,j] = sp.distance.euclidean(cluster[i],cluster[j]);
    return dists.max();

def calculaDistancia2(cluster,centroide) :
    distancias = [];
    for inst in cluster :
        distancias.append(math.pow(sp.distance.euclidean(inst,centroide),2));
    suma = 0.0;
    for dist in distancias :
        suma += dist;
    if len(cluster) == 0 :
        print "HASTA NUNQUI"
    suma *= 1.0/len(cluster);
    return suma;

def print_coh(x,y,s):
    # fig = plt.figure()
    # ax = fig.add_subplot(111)
    # ax.set_xlim(1,21)
    # ax.set_ylim(0,21)
    # print x,y;
    # plt.plot([0,21],[30000,30000],',');
    # for ind in x :
    #     print ind;
    #     print y[ind-2];
    #     ax.plot([ind for i in xrange(ind)],y[ind-2],'*');
    # fig.show();
    minL,meanL,maxL = [],[],[];
    for measure in y :
        minL.append(min(measure));
        maxL.append(max(measure));
        meanp = 0.0;
        for inst in measure:
            meanp += inst;
        meanp *= 1.0/len(measure);
        meanL.append(meanp);
    fig = plt.figure();
    ax = fig.add_subplot(111);
    ax.set_xlim(1,21);
    ax.set_ylim(0,max(maxL)+10000);
    for ind in x :
        ax.plot([ind for i in xrange(ind)],y[ind-2],'b*');
    ax.plot(x,minL,'r',);
    ax.plot(x,maxL,'g');
    ax.plot(x,meanL,'y');
    ax.set_xlabel('clusters');
    ax.set_ylabel(s);
    ax.set_title('Simple XY point plot');
    plt.show();



if __name__ == "__main__":
    trainSet = read_file(sys.argv[1]);
    radio,diametro,distancia2= [],[],[]

    for k in xrange(2,21):
        (clusters,centroides) = kmeans(k,trainSet);
        #print k, centroides;
        radio_local,diametro_local,distancia2_local = [0.0 for i in xrange(k)],[0.0 for i in xrange(k)],[0.0 for i in xrange(k)];
        for x in xrange(k) :
            radio_local[x] = calculaRadio(clusters[x],centroides[x]);
            diametro_local[x] = calculaDiametro(clusters[x]);
            distancia2_local[x] = calculaDistancia2(clusters[x],centroides[x]);
        radio.append(radio_local);diametro.append(diametro_local);distancia2.append(distancia2_local);
    print_coh([k for k in xrange(2,21)],radio,"radio");
    print_coh([k for k in xrange(2,21)],diametro,"diametro");
    print_coh([k for k in xrange(2,21)],distancia2,"distancia2");
    #print radio, diametro, distancia2;
