
from scipy import spatial as sp
import numpy as np
import sys
import operator 
import math

'''
Auxiliary function that process each line
'''
def process_line(line):
    line = line.rstrip('\n');
    elements = line.split(",");
    return elements;

'''
Function that reads the lines of a file given, processes each line and returns a list with the processed data of these.

Arguments:
           filename -- string that identifies the file to process.


Returns: 
           A list whose lenght are the same as the lines processed and that contains one list for each line processed with their dates.
'''
def elems(indice,elem) :
    return elem.index(indice);

def read_file(filename):
    file = open(filename,'r');
    lines = file.readlines();
    attrs = process_line(lines[0]);
    instances = map(process_line,lines[1::]);
    attrib_dic = {};
    classes = [];
    for i in xrange(len(attrs)) :
         valores = [];
         for inst in instances :
             valor = inst[i];
             if valores.count(valor) == 0 :
                 valores.append(valor);
         if i < len(attrs)-1 :
             attrib_dic[attrs[i]] = (i,valores);
         else :
             classes = valores;
    return (instances,attrib_dic,classes);

class ID3():
    def __init__(self) :
          self.id = 0;

    def actualizarId(self):
          self.id += 1;
          return (self.id -1);

    def id3(self,inst,attrib_dic,classes,candidates):
        numClass = len(attrib_dic);
        max = 0;
        for c in classes :
            encontrado = True;
            count = 0;
            for i in inst:
                if i[numClass] != c :
                    encontrado = False; 
                else:
                    count = count + 1;
            if encontrado :
                return Tree(c,None,self.actualizarId());
            else :
                if count > max :
                    claseModa = c;
        if not(encontrado) :
            if candidates == []:
                return Tree(claseModa,None,self.actualizarId());
            else :
                    atrib = self.elegirMejorAtributo(inst,attrib_dic,classes,candidates);
                    candidates.remove(atrib);
                    candidatos_n = candidates[:];
                    arbol = Tree(atrib,None,self.actualizarId());
                    children = [];
                    (loc_atrib,values_atrib) = attrib_dic[atrib];
                    dict = self.crearParticion(inst,loc_atrib,values_atrib);
                    for c in values_atrib :
                        (long,inst_n) = dict[c];
                        if long == 0 :
                            child = Tree(claseModa,c,self.actualizarId);
                        else :
                            child = self.id3(inst_n,attrib_dic,classes,candidatos_n);
                            child.tag = c;
                            child.id = self.actualizarId();
                        children.append(child);
                    arbol.children = children;
        return arbol;

    def elegirMejorAtributo(self,insts,attrib_dic,classes,candidates):
        entr_start = self.entropia(insts,len(attrib_dic),classes);
        max = 0;
        for attrib in candidates:
            (loc,values) = attrib_dic[attrib];
            dict = self.crearParticion(insts,loc,values);
            entr_end = 0.0;
            for c in dict.keys() :
                (num,insts_local) = dict[c];
                entr_local = self.entropia(insts_local,len(attrib_dic),classes);
                entr_end += (num*1.0/len(insts))*entr_local;
            dif = entr_start - entr_end;
            if dif >= max :
                max = dif;
                attrib_n = attrib;
        return attrib_n;
       
    def crearParticion(self,inst,loc,values):
        dict = {};
        for c in values :
            dict[c]=(0,[]);
        for i in inst:
            (ind,instances)=dict[i[loc]];
            ind += 1;
            instances.append(i);
            dict[i[loc]]=(ind,instances);
        return dict;

    def entropia(self,inst,loc_atrib,classes):
        dict = self.crearParticion(inst,loc_atrib,classes);
        N = len(inst)*1.0;
        entropia = 0;
        for c in classes :
            (si,instances) = dict[c];
            if si != 0 :
                entropia -= si/N*math.log(si/N,2);
        return entropia;

    def calcularInstancias(self,valor,atrib,instancias):
        instancias_n = [];
        for inst in instancias :
            if inst[atrib] == valor :
                instancias_n.append(inst);
        return instancias_n;

class Tree:
    def __init__(self) :
        self.root = None;
        self.children = [];
        self.tag = None;
        self.id = 0;
    def __init__(self,root,tag,id):
        self.root = root;
        self.tag = tag;
        self.id = id;
        self.children = [];

    def display(self,i) :
        if self.children == []:
            print tab(i+2),"Clase: ", self.root;
        else :
            print tab(i),"Atributo: ", self.root;
            for child in self.children:
                print tab(i+1),"con valor: ", child.tag;
                child.display(i+2);
      
    def esHoja(self):
        return self.children == [];
    def generatedot(self,fo):
        fo.write("digraph id3{ \n");
        self.generategraph(fo,0);
        fo.write("}");
    def generategraph(self,fo,level):
        if self.esHoja() :
            fo.write("n_%s [style=diagonals,color=green,label=\"%s\"];\n"%(self.id,self.root));
        else :
            fo.write("n_%s [style=solid,color=red,label=\"%s\"];\n"%(self.id,self.root));
            i = 0;
            for child in self.children:
                new_level = i;
                fo.write("n_%s -> n_%s [label=\"%s\"];\n"%(self.id,child.id,child.tag));
                child.generategraph(fo,new_level);
                i += 1;
def tab(i):
    cad="";
    for e in xrange(i):
        cad+="\t";
    return cad;

if __name__ == "__main__":
    algorithm = ID3();
    (inst,attrib_dic,classes) = read_file(sys.argv[1]);
    print attrib_dic
    a = algorithm.id3(inst,attrib_dic,classes,attrib_dic.keys());
    a.display(0);
    fo = open("id3.dot", "wb");
    a.generatedot(fo);
