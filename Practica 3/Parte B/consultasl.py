# -*- coding: utf-8 -*-
"""
Autores: Miguel Isabel y Pablo Gordillo
Grupo 07

Este código es fruto ÚNICAMENTE del trabajo de sus miembros. Declaramos no 
haber colaborado de ninguna manera con otros grupos, haber compartido el ćodigo 
con otros ni haberlo obtenido de una fuente externa.
"""

#################################################################
## Es necesario añadir los parámetros adecuados a cada función ##
#################################################################
import pymongo as pm
import time
import json

client = pm.MongoClient()
db = client.sgdi#_grupo07
    
# 1. Añadir un usuario
def insert_user():
    collection = db.usuarios;
    user = Usuario();
    user.get_user_info();
    user.uid = next_id(collection) + 1;
    dict_user = user.to_dict();
    collection.insert(dict_user);
    print "El usuario con alias",user.alias,"ha sido creado con id:",user.uid;
    print "El objeto introducido es el siguiente: \n";
    print json.dumps(dict_user);
    return json.dumps(dict_user);

# 2. Actualizar un usuario
def update_user():
    collection = db.usuarios;
    alias = raw_input("Introduzca el alias del usuario que quiere modificar: ");
    cursor = collection.find({"alias":alias});
    if cursor.count() > 0:
        cursor = cursor[0];
        user = Usuario();
        user.uid = cursor["_id"];
        user.alias = alias;
        user.get_user_info();
        dict_user = user.to_dict();
        collection.find_one_and_replace({"_id":user.uid},dict_user);
        print "El usuario con alias",alias,"ha sido modificado.";
        print "El nuevo usuario introducido es el que sigue : \n";
        print json.dumps(dict_user);
        return json.dumps(dict_user);
    else :
        print "El alias introducido no existe";

# 3. Añadir una pregunta
def add_question():
    collection = db.entradas;
    pregunta = Pregunta();
    pregunta.get_question_info();
    pregunta.pid = next_id(collection) + 1;
    dict_pregunta = pregunta.to_dict();
    collection.insert(dict_pregunta);
    print "Ha sido añadida una pregunta con id:",pregunta.pid;
    print "La pregunta añadida es la siguiente: \n";
    print json.dumps(dict_pregunta);
    return json.dumps(dict_pregunta);

# 4. Añadir una respuesta a una pregunta.
def add_answer(pregunta):
     collection = db.entradas;
     respuesta = Respuesta();
     cursor = collection.find({"_id":pregunta,"entrada":"pregunta"});
     if cursor.count() > 0 :
         cursor = cursor[0];
         respuesta.get_answer_info(pregunta);
         respuesta.rid = next_id(collection) + 1;
         dict_respuesta = respuesta.to_dict();
         collection.insert(dict_respuesta);
         cursor["numRespuestas"] += 1;
         collection.find_one_and_replace({"_id":pregunta},cursor);
         print "Ha sido añadida una respuesta a la pregunta",pregunta,"con id:",respuesta.rid;
         print "La respuesta añadida es la siguiente: \n";
         print json.dumps(dict_respuesta);
         return json.dumps(dict_respuesta);
     else :
         print "No existe una pregunta con id:", pregunta;

# 5. Comentar una respuesta.
def add_comment(respuesta):
     collection = db.entradas;
     comentario = Comentario();
     cursor = collection.find({"_id":respuesta,"entrada":"respuesta"});
     if cursor.count() > 0 :
         comentario.get_comment_info(respuesta);
         comentario.cid = next_id(collection) + 1;
         dict_comentario = comentario.to_dict();
         collection.insert(dict_comentario);
         print "Ha sido añadido el comentario con id:",comentario.cid;
         print "El comentario introducido es el siguiente: \n";
         print json.dumps(dict_comentario);
         return json.dumps(dict_comentario);
     else :
         print "No existe una respuesta con id", respuesta;

# 6. Puntuar una respuesta.
def score_answer(respuesta):
    collection = db.entradas;
    cursor = collection.find({"_id":respuesta,"entrada":"respuesta"});
    if cursor.count() > 0 :
        puntuacion = Puntuacion();
        puntuacion.get_score_info(respuesta);
        puntuacion.sid = next_id(collection) + 1;
        punt_dict = puntuacion.to_dict();
        cursor = cursor[0];
        collection.insert(punt_dict);
        if punt_dict["tipo"] == "buena" :
            cursor["buenas"] += 1;
        else :
            cursor["malas"] += 1;
        collection.find_one_and_replace({"_id":respuesta},cursor);
        print "La puntuacion ha sido creada con id:",puntuacion.sid;
        print " La puntuacion añadida es la siguiente: \n";
        print json.dumps(punt_dict);
        return json.dumps(punt_dict);
    else :
        print "No existe una respuesta con id",respuesta;
        

# 7. Modificar una puntuacion de buena a mala o viceversa.
def update_score(sid):
    collection = db.entradas;#COMPROBAR QUE ES UNA PUNTUACION.
    cursor = collection.find({"_id":sid,"entrada":"puntuacion"});
    if cursor.count() > 0 :
        cursor = cursor[0];
        #Existe y es unica.#
        cursor2 = collection.find({"_id":cursor["respuesta"]})[0];
        if cursor["tipo"] == "buena" :
            cursor2["malas"] += 1; 
            cursor2["buenas"] -= 1; 
            cursor["tipo"] = "mala";
            print "La puntuacion con id:",sid,"ha pasado de buena a mala";
            print "La puntuación actual queda como sigue: \n";
            print json.dumps(cursor);
        else :
            cursor2["buenas"] += 1; 
            cursor2["malas"] -= 1; 
            cursor["tipo"] = "buena";
            print "La puntuacion con id:",sid,"ha pasado de mala a buena";
            print "La puntuación actual queda como sigue: \n";
            print json.dumps(cursor);
        collection.find_one_and_replace({"_id":sid},cursor);
        collection.find_one_and_replace({"_id":cursor["respuesta"]},cursor2);
        return json.dumps(cursor);
    else : 
        print "No existe una puntuacion con id",sid;


# 8. Borrar una pregunta junto con todas sus respuestas, comentarios y 
# puntuaciones
def delete_question(question_id):
    collection = db.entradas;
    collection.remove({"_id":question_id});
    collection.remove({"pregunta":question_id});
    collection.remove({"pregunta.id_pregunta":question_id});


# 9. Visualizar una determinada pregunta junto con todas sus contestaciones
# y comentarios. A su vez las contestaciones vendran acompañadas de su
# numero de puntuaciones buenas y malas.
def get_question(question_id):
    lista = []
    collection = db.entradas;
    cursor = collection.find({"_id":question_id,"entrada":"pregunta"});
    if cursor.count() > 0 :
        pregunta = cursor[0];
        cursorRespuesta = collection.find({"pregunta":question_id,"entrada":"respuesta"},{"_id":1,"texto":1,"buenas":1,"malas":1});
        print "Titulo de la pregunta: ",pregunta["titulo"];
        print "Cuyo texto es:",pregunta["texto"];
        lista.append(json.dumps(pregunta));
        if cursorRespuesta.count() > 0:
            for i in cursorRespuesta : 
                print "\t Respuesta : ",i["texto"];
                print "\t Numero de puntuaciones buenas ",i["buenas"];
                print "\t Numero de punuaciones malas ", i["malas"];
                lista.append(json.dumps(i));
                cursorComentario = collection.find({"respuesta":i["_id"],"entrada":"comentario"},{"texto":1});
                if cursorComentario.count() > 0:
                    for j in cursorComentario :
                        print "\t \t Comentario : ",j["texto"];
                        lista.append(json.dumps(j))
                else :
                    print "\t \t  La respuesta no tiene comentarios"
        else : print "\ t Esta pregunta no tiene respuestas.";
        dicti={}
        dicti["result"]=lista;
        return json.dumps(dicti);
    else :
        print "La pregunta",question_id,"no existe."

# 10. Buscar preguntas con unos determinados tags y mostrar su titulo, su autor
# y su numero de contestaciones.
def get_question_by_tag(tags):
    lista = [];
    collection = db.entradas;
    cursor = collection.find({"tags":{"$all":tags}},{"titulo":1,"usuario":1,"numRespuestas":1});
    if cursor.count() > 0 :
        j = 1;
        print "Con la lista de tags",tags, "se han encontrado:"; 
        for i in cursor:
            print "Pregunta ", j;
            print "\t Título: ", i["titulo"];
            usuario = db.usuarios.find({"_id":i["usuario"]})[0];
            print "\t Publicado por: ", usuario["alias"];
            print "\t Con", i["numRespuestas"],"respuestas";
            j += 1;
            print "La representación de pregunta" , "es : \n";
            print json.dumps(i);
            lista.append(json.dumps(i));
        dicti = {};
        dicti["result"]=lista;
        return json.dumps(dicti);
    else :
        print "No se ha encontrado ninguna pregunta para la lista de tags: "
        print tags;
                              
# 11. Ver todas las preguntas o respuestas generadas por un determinado usuario.
def get_entries_by_user():
    lista = [];
    collection = db.entradas;
    cursor = get_alias();
    cursorEntradas = db.entradas.find({"$and":[{"usuario":cursor["_id"]},{"$or":[{"entrada":"pregunta"},{"entrada":"respuesta"}]}]}).sort([("fecha.anno",-1),("fecha.mes",-1),("fecha.dia",-1)]);
    if cursorEntradas.count() > 0 :
        for i in cursorEntradas:
            lista.append(json.dumps(i));
            if i["entrada"] == "pregunta" :
                print "El usuario realizo la pregunta:",i["titulo"];
                print "cuyo texto es:",i["texto"];
            else :
                print "El usuario respondio a la pregunta:",i["pregunta"];
                print "con el siguiente texto:",i["texto"];
        dicti = {}
        dicti["results"] = lista;
        return json.dumps(dicti);
    else :
        print "El usuario",cursor["alias"],"no tiene entradas.";
    

# 12. Ver todas las puntuaciones de un determinado usuario ordenadas por 
# fecha. Este listado debe contener el tıtulo de la pregunta original 
# cuya respuesta se puntuo.
def get_scores():
    collection = db.entradas;
    cursor = get_alias();
    lista = [];
    alias = cursor["alias"];
    cursor = collection.find({"usuario":cursor["_id"],"entrada":"puntuacion"}).sort([("fecha.anno",-1),("fecha.mes",-1),("fecha.dia",-1),("_id",-1)]);
    if cursor.count()>0 :
        print "Las puntuaciones asociadas al usuario ",alias," son:";
        for i in cursor :
            print "Puntuacion numero ",i["_id"], "realizada a la pregunta con id ", i["pregunta"]["id_pregunta"],": \n";
            print "\t Titulo de la pregunta: ", i["pregunta"]["titulo"];
            print "\t Puntuacion: ", i["tipo"];
            print "\t Representacion de la puntuacion: ";
            print json.dumps(i);
            lista.append(json.dumps(i));
        dicti={};
        dicti["result"]= lista;
        return json.dumps(dicti);
    else :
        print "El usuario introducido no ha puntuado ninguna respuesta"


# 13. Ver todos los datos de un usuario.
def get_user():
    collection = db.usuarios;
    us = get_alias();
    print "Los datos del usuario con alias",us["alias"], "son los siguientes: ";
    print "\t Num ID: ",us["_id"];
    aps = "";
    for ap in us["apellidos"]:
        aps += ap + " ";
    print "\t Name: ", us["name"], aps;
    aps = "";
    for ap in us["experiencia"]:
        aps += ap + ", ";
    print "\t Experiencia: ", aps[:-2];
    print "\t Fecha (dia-mes-anno) ", us["fecha"]["dia"], "-",us["fecha"]["mes"],"-",us["fecha"]["anno"];
    print "\t Direccion (Codigo,Ciudad,Pais): ", us["direccion"]["codigo"],"-",us["direccion"]["ciudad"],"-",us["direccion"]["pais"];
    print json.dumps(us);
    return json.dumps(us);

# 14. Obtener los alias de los usuarios expertos en un determinado tema.
def get_users_by_expertise(tema):
    lista = []
    collection = db.usuarios;
    cursor = collection.find({"experiencia":tema},{"alias":1});
    if cursor.count() > 0 :
        print "Los usuarios expertos para el tema", tema, "son: ";
        for us in cursor:
            lista.append(json.dumps(us));
            print "\t", us["alias"];
            print json.dumps(us);
        dicti = {}
        dicti["results"] = lista;
        return json.dumps(dicti);
    else :
        print "No hay usuarios expertos en el tema",tema;

# 15. Visualizar las n preguntas mas actuales ordenadas por fecha, incluyendo
# el numero de contestaciones recibidas.
def get_newest_questions(n):
    lista = [];
    collection = db.entradas;
    actuales = collection.find({"entrada":"pregunta"}).sort([("fecha.anno",-1),("fecha.mes",-1),("fecha.dia",-1),("_id",-1)]).limit(n);
    for i in actuales:
        lista.append(json.dumps(i));
        print "Pregunta:",i["titulo"];
        print "\t cuyo texto es:",i["texto"];
        print "\t y ha recibido", i["numRespuestas"], "respuestas";
    dicti = {};
    dicti["result"] = lista;
    return json.dumps(dicti);

# 16. Ver n preguntas sobre un determinado tema, ordenadas de mayor a menor por
# numero de contestaciones recibidas.
def get_questions_by_tag(tema,n):
    collection = db.entradas;
    lista = [];
    preguntas = collection.find({"entrada":"pregunta","tags":tema}).sort([("numRespuestas",-1)]).limit(n);
    if preguntas.count() > 0 :
        for i in preguntas:
            lista.append(json.dumps(i));
            print "El usuario realizo la pregunta:",i["titulo"];
            print "cuyo texto es:",i["texto"];
            print "y ha recibido ", i["numRespuestas"], "respuestas";
        dicti = {}
        dicti["results"] = lista;
        return json.dumps(dicti);
    else : 
        print "No se han encontrado preguntas con el tema", tema;
################################################################################
############################  FUNCIONES AUXILIARES  ############################
################################################################################

# Incluir aqui el resto de funciones necesarias

def delete_user():
    collection = db.usuarios;
    alias = raw_input("Introuzca el alias del usuario que quiere eliminar: ");
    collection.remove({"alias":alias});

def next_id(collection):
    cursor = collection.find({}).sort([("_id",-1)]).limit(1)[0];
    return cursor["_id"];

def get_alias():
    alias = "";
    while alias == "":
        alias = raw_input("Introduzca su alias de usuario: ");
        cursor = db.usuarios.find({"alias":alias});
        if cursor.count() == 0 :
            print "El usuario",alias,"no existe. Introduzca su usuario.";
            alias = "";
    return cursor[0];
     
class Usuario:
    
    def __init__(self):
        self.alias = "";
        self.name = "";
        self.apellidos = "";
        self.uid = 0;

    def create(self,alias,name,apellidos,experiencia,dia,mes,anyo,pais,ciudad,codigo):
        self.alias = alias;
        self.name = name;
        self.apellidos = apellidos;
        self.experiencia = experiencia;
        self.fecha = {"dia":dia,"mes":mes,"anno":anyo};
        self.direccion = {"pais":pais,"ciudad":ciudad,"codigo":codigo};
        
    def to_dict(self):
        return {"_id":self.uid,
                "alias":self.alias,
                "name":self.name,
                "apellidos":self.apellidos,
                "experiencia":self.experiencia,
                "fecha":self.fecha,
                "direccion":self.direccion}
                             

    def get_user_info(self):           
        alias = "";
        while alias == "":
            alias = raw_input("Introduzca su alias: ");
            cursor = db.usuarios.find({"alias":alias});
            if cursor.count() > 0 and self.alias != alias :
                print "El alias",alias,"ya esta seleccionado, pruebe otro.";
                alias = "";
        self.alias = alias;
        self.name = raw_input("Introduzca su nombre: ");
        ap1 = raw_input("Introduzca su 1º apellido: ");
        ap2 = raw_input("Introduzca su 2º apellido: ");
        self.apellidos = [ap1,ap2];
        exps = [];
        exp = "";
        while exp != "." : 
            exp= raw_input("Introduzca un tema (Para terminar pulse .): ");
            exps += [exp];
        self.experiencia = exps[:-1];
        f = time.strftime("%x")
        f_aux = f.split("/")
        mes = int(f_aux[0])
        dia = int(f_aux[1])
        anyo = int(f_aux[2])+2000
        self.fecha = {"dia":dia,"mes":mes,"anno":anyo};
        pais = raw_input("Introduzca el pais: ");
        ciudad = raw_input("Introduzca la ciudad: ");
        codigo = raw_input("Introduzca el codigo: ");
       
        self.direccion = {"pais":pais,"ciudad":ciudad,"codigo":codigo};

class Pregunta:
    
    def __init__(self):
        self.numRespuesta = 0;

        
    def to_dict(self):
        return {"_id":self.pid,
                "titulo":self.titulo,
                "usuario":self.usuario,
                "tags":self.tags,
                "texto":self.texto,
                "fecha":self.fecha,
                "numRespuestas":self.numRespuesta,
                "entrada":"pregunta"};

    def get_question_info(self):           
        cursor = get_alias();
        self.usuario = cursor["_id"];
        self.titulo = raw_input("Introduzca el titulo: ");
        self.texto = raw_input("Introduzca el texto de la pregunta : ");
        exps = [];
        exp = "";
        while exp != "." : 
            exp= raw_input("Introduzca una etiqueta (Para terminar pulse .): ");
            exps += [exp];
        self.tags = exps[:-1];
        f = time.strftime("%x")
        f_aux = f.split("/")
        mes = int(f_aux[0])
        dia = int(f_aux[1])
        anyo = int(f_aux[2])+2000
        self.fecha = {"dia":dia,"mes":mes,"anno":anyo};

class Respuesta:
    
    def __init__(self):
        self.buenas= 0;
        self.malas = 0;
  
   
    def to_dict(self):
        return {"_id":self.rid,
                "pregunta":self.pregunta,
                "usuario":self.usuario,
                "texto":self.texto,
                "buenas":self.buenas,
                "malas":self.malas,
                "fecha":self.fecha,
                "entrada":"respuesta"};
 
    def get_answer_info(self,pregunta): 
        self.pregunta = pregunta;
        cursor = get_alias();
        self.usuario = cursor["_id"];
        self.texto = raw_input("Introduzca el texto de la respuesta: ");
        f = time.strftime("%x")
        f_aux = f.split("/")
        mes = int(f_aux[0])
        dia = int(f_aux[1])
        anyo = int(f_aux[2])+2000
        self.fecha = {"dia":dia,"mes":mes,"anno":anyo};
  
class Comentario:
    
    def __init__(self):
        self.pregunta = "";
        self.respuesta = "";
        self.usuario = "";

   
    def to_dict(self):
        return {
            "_id":self.cid,
            "pregunta":self.pregunta,
            "respuesta":self.respuesta,
            "usuario":self.usuario,
            "texto":self.texto,
            "fecha":self.fecha,
            "entrada":"comentario"};
 
    def get_comment_info(self,respuesta):      
        cursor = get_alias();
        self.usuario = cursor["_id"];
        collection = db.entradas;
        cursor = collection.find({"_id":respuesta})[0];
        self.pregunta = cursor["pregunta"];
        self.respuesta = respuesta;
        self.texto = raw_input("Introduzca el texto del comentario : ");
        f = time.strftime("%x")
        f_aux = f.split("/")
        mes = int(f_aux[0])
        dia = int(f_aux[1])
        anyo = int(f_aux[2])+2000
        # dia = raw_input("Introduzca el dia de creacion: ");
        # mes = raw_input("Introduzca el mes de creacion: ");
        # anyo = raw_input("Introduzca el anno de creacion: ");
        self.fecha = {"dia":dia,"mes":mes,"anno":anyo};

class Puntuacion:
    
    def __init__(self):
        self.pregunta = "";
        self.respuesta = "";
        self.usuario = "";

   
    def to_dict(self):
        return {"_id":self.sid,
                "pregunta":self.pregunta,
                "respuesta":self.respuesta,
                "usuario":self.usuario,
                "tipo":self.tipo,
                "fecha":self.fecha,
                "entrada":"puntuacion"};
 
    
    def get_score_info(self,respuesta):       
        cursor = get_alias();
        self.usuario = cursor["_id"];
        collection = db.entradas;
        cursor = collection.find({"_id":respuesta})[0];
        cursor2 = collection.find({"_id":cursor["pregunta"]})[0];
        self.pregunta = {"id_pregunta":cursor2["_id"],"titulo":cursor2["titulo"]};
        self.respuesta = respuesta;
        tipo = "";
        while tipo != "buena" and tipo != "mala": 
            tipo = raw_input("Introduzca si es buena o mala : ");
        self.tipo = tipo;
        f = time.strftime("%x")
        f_aux = f.split("/")
        mes = int(f_aux[0])
        dia = int(f_aux[1])
        anyo = int(f_aux[2])+2000
        # dia = raw_input("Introduzca el dia de creacion: ");
        # mes = raw_input("Introduzca el mes de creacion: ");
        # anyo = raw_input("Introduzca el anno de creacion: ");
        self.fecha = {"dia":dia,"mes":mes,"anno":anyo};

if __name__ == "__main__":
    #get_uses_by_expertise("tema");
#    insert_user();
#    get_user("edumoran");
#   update_score(13);
#   delete_question(46);
#   delete_user();
#   get_entries_by_user(1);
#   get_scores(1);
#   get_uses_by_expertise("Prolog");
#   get_questions_by_tag("matematicas",2);
#    get_entries_by_user();
#   get_newest_questions(5);
#    add_question();
#    get_user();
#   update_user();
#    add_answer(1);
#    add_comment(47);
#    get_question(46);
#     score_answer(47);
#     get_newest_questions(2);
#    insert_user();
#    update_user();
#    add_question();
#    add_answer(46);
#    add_answer(50);
#    add_comment(50);
#    add_comment(47);
#    score_answer(47);
#    score_answer(50);
#    score_answer(48);
#    update_score(51);
#     update_score(51);
#    get_question(46);
#    update_score(80);
#     score_answer(47);
#     get_newest_questions(2);
#    get_users_by_expertise("Prolog");
    get_questions_by_tag("matematicas",2);
