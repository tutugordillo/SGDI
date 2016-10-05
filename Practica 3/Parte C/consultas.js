/*******************************************************************************
**************************** AGGREGATION FRAMEWORK *****************************
*******************************************************************************/

// Listado de pais-numero de usuarios ordenado de mayor a menor por numero de 
// usuarios.
function agg1(){
  return  db.agg.aggregate([{$group:{_id:"$country",numUsers:{$sum:1}}},{$sort:{numUsers:-1}}]);
   
}


// Listado de pais-numero total de posts de los 3 paises con mayor numero total 
// de posts, ordenado de mayor a menor por numero de posts.
function agg2(){
    return db.agg.aggregate([{$group:{_id:"$country",numUsers:{$sum:"$num_posts"}}},{$sort:{numUsers:-1}},{$limit:3}]);
	
}

  
// Listado de aficion-numero de usuarios ordenado de mayor a menor numero de 
// usuarios.
function agg3(){
    return db.agg.aggregate([{$unwind:"$likes"},{$group:{_id:"$likes",numUsers:{$sum:1}}},{$sort:{numUsers:-1}}]);
    
}  
  
  
// Listado de aficion-numero de usuarios restringido a usuarios espanoles y
// ordenado de mayor a menor numero de usuarios.
function agg4(){
  return db.agg.aggregate([{$match:{country:"Spain"}},{$unwind:"$likes"},{$group:{_id:"$likes",numUsers:{$sum:1}}},{$sort:{numUsers:-1}}]);
  
}



/*******************************************************************************
********************************** MAPREDUCE ***********************************
*******************************************************************************/
  
// Listado de aficion-numero de usuarios restringido a usuarios espanoles.
function mr1(){

    var mapper = function(){
	if(this.likes != null){
	    this.likes.forEach(function(entry){emit(entry,1);});
}

};

    var reducer = function(key,values){
	return Array.sum(values)
};
	return db.agg.mapReduce(mapper,reducer,{out:"total"}).find();
}


// Listado de numero de aficiones-numero de usuarios, es decir, cuAntos
// usuarios tienen 0 aficiones, cuantos una aficion, cuantos dos aficiones, etc.
function mr2(){

    var mapper = function(){
    if (this.likes == null){
	emit(0,1);
    }else {
	emit(this.likes.length,1)
}

};

    var reducer = function(key,values){
	return Array.sum(values);
};

    return db.agg.mapReduce(mapper,reducer,{out:"total"}).find();
}


// Listado de pais-numero de usuarios que tienen mas posts que contestaciones.
function mr3(){

    var mapper = function(){
	if (this.num_posts>this.num_answers){
	    emit(this.country,1);
	}

};

    var reducer = function(key,values){
	return Array.sum(values);

};
    return db.agg.mapReduce(mapper,reducer,{out:"total"}).find();
}


// Listado de pais-media de posts por usuario.
function mr4(){
    var mapper = function(){
	emit(this.country,this.num_posts);

};

    var reducer = function(key,values){
	return Array.sum(values)/values.length;

};
    
    return db.agg.mapReduce(mapper,reducer,{out:"total"}).find();
}




