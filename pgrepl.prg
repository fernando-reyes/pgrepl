#include "fileio.ch"
#include "../ivutils.ch"
#include "hbxml.ch"
#include "inkey.ch"
#include "signal.ch"
#include "xhb.ch"
#include "hbcompat.ch"

#define NODE_CLEAN 	-1
#define NODE_OFFLINE 	-2
#define REG_NODELETE	-1

//150211 FRB
//#define VERSION 1.0
//080612 se agrega "txid" para agrupar y ejecutar por transaccion
//#define VERSION 1.3
//280812 portado a HB
//#define VERSION 2.0
//071112 control de transacciones pendientes
//#define VERSION 2.5
//130213 control de tx pendientes mediante bloqueo de tabla
//#define VERSION 2.6

//270213 se reemplaza TX en vez de RX para testigo en esclavo
//       limpieza de codigo
//	 entrega mayor info en caso de excepcion
#define VERSION 2.7


static;
    sqlGetReplica :=[select id,sql,ts,"table", txid	] + ;	//transacciones pendientes para replicar
		    [  from replica.replica_log         ] + ;
		    [ where not replicated              ] + ;
		    [   and txid <= $1 			] + ;
		    [ order by txid,id			] , ;
    sqlDelReplica :=[delete 				] + ;	//para eliminar regsitros ya replicados
		    [  from replica.replica_log 	] + ;
		    [ where replicated 			] + ;
		    [   and ts  < '$1'  		] , ;
    sqlMarkLocal  :=[update replica.replica_log 	] + ;	//para marcar la transaccion como ya replicada en todos los nodos
		    [   set replicated = true   	] + ;
		    [ where not replicated      	] + ;
		    [   and txid       = $1		] , ;
    sqlGetLastID  :=[select last 			] + ;	//obtencion del ultimo registro replicado en el esclavo
		    [  from replica.replica_nodes 	] + ;
		    [ where origin=$1			] , ;
    sqlUpdLast	  :=[update replica.replica_nodes	] + ;	//identificacion del ultimo registro replicado en el esclavo
		    [   set last   = $1, 		] + ;
		    [       ts     = now()		] + ;
		    [ where origin = $2			] , ;
    sqlInsLast	  :=[insert 				] + ;	//identificacion del ultimo registro replicado en el esclavo
		    [  into replica.replica_nodes	] + ;
		    [	    (origin,last,ts)		] + ;
		    [values ('$1',$2,now())		] , ;
    sqlGetRplTX	  :=[select txid 			] + ;
		    [  from replica.replica_log 	] + ;
		    [ where id=$1 			] , ;
    sqlGetMaxTX	  :=[  lock table replica.replica_log 	] + ;	//bloqueamos, asegurando que no hay nada pendiente
		    [    in share mode nowait;		] + ;
		    [select txid 			] + ;	//obtenemos la ultima TX confirmada y sin TX pendientes por registrar en replica
		    [  from replica.replica_log 	] + ;
		    [ where not replicated		] + ;
		    [ order by txid desc,id desc	] + ;
		    [ limit 1				]


static lastSignal:=0	//SIGHUP,SIGALRM,SIGTERM,etc.,...
static group		//lista con nodos para replicacion

//dbHost: identificacion (IP o hostname) del host maestro dentro de la lista de nodos en .conf ...
func main(dbHost) 
    local config,node,i,data,nodeCount,tables,tab,sqlLimit,txtout
	  
    initialize()
    
    if dbHost == nil 
	?  exename()+' <master host> [<config_file> [<debug=N/y>]]' 
	?  'NOTE: SIGHUP  = Reload params' 
	?  '      SIGALRM = Display info'
	?  '      OTHER   = Quit' 
	?  'VERSION:'+strTrim(VERSION)
	?
	retu
    endif

    checkOnce()

    //obteniendo configuracion
    config:=getConfig()
    
    if empty( config )
	retu
    endif

    //seccion global de configuracion
    global:=getGlobalConfig(config)
    if empty( global )
	retu
    endif
    
    //normalizando la configuracion de los nodos segun parametros por defecto tomados de seccion 'global'
    group    := config:getNodeByName:eval('group')
    
    thisNode := nil	//solo para que veas que esta en nil...
    
    nodeCount:=len(group:nodes)-1
    
    //for each node in group:nodes
    for i:=0 to nodeCount
    
	node := group:nodes[ i + 1 ]
	node:id := i + 1

	node:username	:= global:username
	node:port	:= mapGet( node, 'PORT', 	mapGet( group , 'DEFAULT_PORT' , '5432') )
	node:password	:= mapGet( node, 'PASSWORD', 	mapGet( group , 'DEFAULT_PASSWORD' ,'postgres' ) )
	node:database	:= mapGet( node, 'DATABASE', 	mapGet( group , 'DEFAULT_DATABASE' ,'postgres' ) )

	node:txid	:= 0	///
	node:lastMsg	:= ''				//ultimo mensaje de aviso o error enviado por este nodo
	node:lastMsgTime:= 0				//momento del ultimo mensaje
	//node:bypassed   := -1
	
	tables		:= explode( mapGet( node, 'TABLES', '' ) , ',' )
	
	node:tables	:= NIL
	
	if tables[1] != ''
	    node:tables := map()
	    
	    //asignamos las tablas especificadas para replicar (por defecto son todas)
	    aEval( tables , {|val| node:tables[ alltrim(val) ] := .t. } )
	endif

	node:text := node:host := alltrim( node:host )
	if node:text == dbHost	//encontramos nuestro nodo maestro...
	    thisNode := node
	    thisNode:rxid := 0
	endif

    next

    if thisNode == nil
	debug( 'No se encontro '+dbHost+' en la lista de nodos...' )
	debug( '' )
	retu
    endif

    debug('Iniciando servicio')
    debug('')

    //quitamos al nodo maestro de la lista de replicacion 
    aDelete( group:nodes , thisNode:id )

    thisNode:conn := getConnection( thisNode )

    if thisNode:conn==nil	//si no se puede conectar al nodo maestro se sale...
	retu
    endif
    
    //consulta para obtener la identificion del ultimo registro replicado en un nodo
    sqlGetLastID:=strTran(sqlGetLastID,'$1',"'"+dbHost+"'")
    
    txtout   := 0

    while .t.		//main loop

	( processSignalInfo() .and. processSignalHUP() )

	//obtenemos el id de la ultima TX, asi seleccionaremos solo los registro "commiteados"
	//getMaxTX trata de bloquear para asegurar que no hay ada pendiente...
	if ( txid := getMaxTX()  ) <= 0

	    if txid == -1

		if txtout == 0
		    txtout   := now() + global:transaction_timeout
		elseif now() > txtout				//y ya paso el tiempo...

		    debug("Esperando termino de TX",.t.)
		    txtout := 0
		endif
	    endif

	    sweepOldTX()

	    sleep(global:interval)	//delay para no acaparar CPU...
	    loop
	endif

	txtout := 0

	//obtenemos los registros a replicar, no mover de aqui ya que puede ser cambiado por configuracion
	sqlLimit := mapGet( global , 'LIMIT' , '' )
	sqlLimit := if( empty(sqlLimit) , '' , 'limit ' + sqlLimit )

	if ( data := executeSql( thisNode , strTran( sqlGetReplica, '$1', strTrim(txid) ) + sqlLimit ) ) == nil
	    loop
	endif

	if !data:eof()

	    aEval( group:nodes , {|node| 	 ;
		    node:retryAt 	:= 0	,;
		    if( ( node:conn := getConnection(node) ) == nil , node:retryAt := now() + global:connection_retry_delay , ),;
		    node:skipThis := ( node:conn == nil .or. !getLastRepId( node ) );
		} )

	    while !data:eof() 

		thisNode:txid 		:= data:fieldGet('txid')
		thisNode:recno 		:= data:recno()		//primer registro de la tx en curso (en el mismo recordset puede haber mas de una tx)
		thisNode:replNodes	:= 0			//cantidad de nodos replicados	    

		for each node in group:nodes

		    processSignalInfo(node)

		    if  node:skipThis    .or. ;
			node:retryAt > now() 			//nodo con problemas... 
			loop					//pasamos al siguiente nodo...
		    endif

		    data:goto( thisNode:recno )			//vamos al primer registro de la TX

		    //si ya fue replicado en este nodo...
		    if node:txid >= data:fieldGet('txid') /*.or. data:eof()*/ 
? dtos(date()  )+':'+time()+"  OLDER:",;
    node:host+':'+node:port,;
    ',node:txid =',node:txid,;
    ',data:fieldGet("rxid") =',data:fieldGet('id'),;
    ',data:fieldGet("txid") =',data:fieldGet('txid')
//<FRB:140414> quitamos marca de replicado, recordar que el id de transaccion puede ser obtenido antes que otro y sin bloquear replica_log en ningun momento
//		    	    thisNode:replNodes++
//		    	loop
//</FRB>    
		    endif

		    thisNode:txid := data:fieldGet('txid')
		    //thisNode:rxid := data:fieldGet('id')	//SOLO INFORMATIVO
		    
		    node:conn:conn:startTransaction()				// iniciamos la transaccion
		    
		    tryReplicateTx(node,data)

		    if node:skipThis .or. executeSql( node, sqlUpdLast ,;		//guardando el id de replicacion del registro en la misma transaccion
							{ strTrim( thisNode:txid ), thisNode:text } ) == nil

? dtos(date()  )+':'+time()+"  ROLLBACK:",node:host+':'+node:port,thisNode:txid,thisNode:rxid

			node:conn:conn:rollBack()
		    else	
			
			node:conn:conn:commit()

? dtos(date()  )+':'+time()+"  COMMIT:",node:host+':'+node:port,thisNode:txid,thisNode:rxid

			//node:rxid  := thisNode:rxid		//ultimo replicado
			node:txid    := thisNode:txid		//ultimo replicado 
			thisNode:replNodes++
		    endif	    
		    
		next	//for node...

		//Si le fue mal en todo entonces debemos reiniciar el ciclo
		if thisNode:replNodes == 0
		    exit
		endif

		//si recorrio todos los nodos entonces solo nos queda marcar el lote como replicado...
		if thisNode:replNodes == nodeCount .and. executeSql(thisNode, sqlMarkLocal, {strTrim(thisNode:txid) } ) == nil
	    	    //si replNodes == nodeCount entonces DEBE ser marcado como replicado,
	    	    //si no se pudo entonces que lo vea el dba y como no podemos replicar
	    	    //los que siguen entonces nos caemos (mando mensaje)...
	    	    quit
		endif
		
		//y vamos a la siguiente TX
		while !data:eof() .and. data:fieldGet('txid') == thisNode:txid
		    data:skip()
		enddo
		
	    enddo

	    aEval( group:nodes , {|node| if( node:conn != nil , node:conn:conn:destroy() , ) } )
	    
	endif
	
	data:destroy()
	
    enddo			//main loop

retu


func tryReplicateTx(node,data)
//? dtos(date()  )+':'+time()+" BEGIN :",data:fieldGet('txid'),'/',data:fieldGet('id'),thisNode:txid,'/',thisNode:rxid
    while !node:skipThis .and. !data:eof() .and. data:fieldGet('txid') == thisNode:txid		    	
	thisNode:rxid := data:fieldGet('id')			// obtenemos el registro de replicacion
	processSignalInfo(node)
//? dtos(date()  )+':'+time()+"   EXEC:",thisNode:txid,thisNode:rxid
	//validamos si hay que replicar esta tabla o no...
	if ( node:tables == nil .or. mapGet(node:tables,data:fieldGet('table'),.f.) )
	    if executeSql( node , data:fieldGet('sql') ) == nil	//,data:fieldGet('table'))	//replicando registro...
		node:skipThis := .t.					// que se salte este nodo hasta el siguiente lote de replicacion (data)
		exit
	    endif
	endif
	data:skip()
    enddo    
retu


func sweepOldTX()
	    //eliminamos los registros viejos (regLifeTime) que hayan sido replicados totalmente...
    if global:regLifeTime != REG_NODELETE.and.;	//REG_NODELETE = no eliminar nada, modo seguro, pero se puede llenar el disco
       now() > global:next_sweep
	if executeSql( thisNode , strTran( sqlDelReplica, '$1', getLastDateTime( global:regLifeTime ) ) ) == nil
    	    //si falla aqui entonces algo malo paso... que lo vea el dba...
    	    quit
	endif
	global:next_sweep := now() + global:sweep_interval
    endif
retu


func getConnection(node)
    local conn,o,errB    
    //errB:=errorBlock({||nil})
    //begin sequence	
    try
	conn := {"CONN"=>TPQServer():new( ;
        	    node:text ,;
		    node:database,;
		    node:userName,;
		    node:password,;
		    val(node:port),,;
		    mapGet(node,'OPTIONS', mapget( group , 'DEFAULT_OPTIONS', nil )  )),;
		"HOST"=>,;
		"PORT"=>}

	if !empty( conn:conn:errorMsg() )

    	    debug( 'Error al tratar de conectar con '+node:text+':'+node:port+' '+ CRLF + conn:conn:errorMsg() ,.t.,node)
	    conn := nil
	else
	    conn:conn:query([set DATESTYLE  to YMD])
	    conn:host:=node:text
	    conn:port:=node:port
	endif
    catch o
    //recover using o
	debug( 'Error al tratar de conectar con '+node:text+':'+node:port+' '+o:operation ,.t.,node)
	conn := nil
    end try	
    //endseq
    //errorBlock(errB)
retu conn

func getMaxTX()
    local ret := -1,data

    thisNode:conn:conn:query("begin")

    data := executeSql( thisNode , sqlGetMaxTX ,, .f.)

    if data != nil
	ret := if( data:eof() , 0 , data:fieldGet('txid') )    
	data:destroy()    	
    endif
    thisNode:conn:conn:query("rollback")
retu ret

func executeSql(node,sql,params,notif)
    local row,rs,o,i,errB,err //,CRLF := chr(13) 

    //errB:=errorBlock({||nil})

    //begin sequence
    try

	    rs := node:conn:conn:query(sql,params)

	    if !empty( err := PQRESULTERRORFIELD( rs:pQuery, asc('C') ) )

		if notif == nil .or. notif

		    debug(  'ERROR ' + ;
			    'HOST=' + node:host + ':' + node:port + '/' + node:database + CRLF + ;
			    ' STMT:' + left(sql,4096) + CRLF + ; 
			    ' PG_DIAG_SQLSTATE______________:' + err + CRLF + ;
			    ' PG_DIAG_SEVERITY______________:' + PQRESULTERRORFIELD( rs:pQuery, asc('S') ) + CRLF + ;
			    ' PG_DIAG_MESSAGE_PRIMARY_______:' + PQRESULTERRORFIELD( rs:pQuery, asc('M') ) + CRLF + ;
			    ' PG_DIAG_MESSAGE_DETAIL________:' + PQRESULTERRORFIELD( rs:pQuery, asc('D') ) + CRLF + ;
			    ' PG_DIAG_MESSAGE_HINT__________:' + PQRESULTERRORFIELD( rs:pQuery, asc('H') ) + CRLF + ;
			    ' PG_DIAG_STATEMENT_POSITION____:' + PQRESULTERRORFIELD( rs:pQuery, asc('P') ) + CRLF + ;
			    ' PG_DIAG_INTERNAL_POSITION_____:' + PQRESULTERRORFIELD( rs:pQuery, asc('p') ) + CRLF + ;
			    ' PG_DIAG_INTERNAL_QUERY________:' + PQRESULTERRORFIELD( rs:pQuery, asc('q') ) + CRLF + ;
			    ' PG_DIAG_CONTEXT_______________:' + PQRESULTERRORFIELD( rs:pQuery, asc('W') ) + CRLF + ;
			    ' PG_DIAG_SOURCE_FILE___________:' + PQRESULTERRORFIELD( rs:pQuery, asc('F') ) + CRLF + ;
			    ' PG_DIAG_SOURCE_LINE___________:' + PQRESULTERRORFIELD( rs:pQuery, asc('L') ) + CRLF + ;
			    ' PG_DIAG_SOURCE_FUNCTION_______:' + PQRESULTERRORFIELD( rs:pQuery, asc('R') ) ;
			    ,.t.,node)
			    
		endif
		rs:destroy()
		rs := nil	 
            endif
    catch o
    //recover using o
	debug(  'ERROR_DESCONOCIDO '+;
		'HOST:' + node:conn:host + '/' + node:conn:port + CRLF + ;
		' STMT:' + left(sql,4096) ,.t.,node)
	rs := nil
    //endseq
    end try
    //errorBlock(errB)

return rs

func getLastRepId(node)
    local data:=executeSql(node,sqlGetLastID) , res := .f.
    if data != nil
	if data:eof()	//insertamos el registro del ultimo identificador de registro de replicacion en el esclavo    
	    res := executeSql( node,strATran(sqlInsLast,{"$1","$2"},{thisNode:text,'-1'}) ) != nil
	else
    	    node:txid := data:fieldGet('last')   	//tenemos el ID del ultimo registro de replicacion
	    res := .t.
	endif
	data:destroy()
    endif
retu res

func debug(msg,callOnError,node)

    if msg==nil 			//reinicia servicio de logging
//? "RESET"    
	thisNode:lastMsg:='@'
	thisNode:lastMsgTime := 0
	aEval( group:nodes, {|x|x:lastMsg:='@'} )
	retu
    endif

    if node == nil		//problema global / independiente del nodo
	node := thisNode
    endif

    if node == nil		//no alcanzo a entrar ni nada
	retu
    endif

    //validamos para no repetir el mensaje...
//?  ' PRIMERA ',   msg == node:lastMsg, '[' , left(msg,20), '][',left(node:lastMsg,20),']',node:text,now() , node:lastMsgTime , global:repeatErrorDelay
//? 'primera ',msg == node:lastMsg , now() > node:lastMsgTime + global:repeatErrorDelay
    if msg == node:lastMsg .and. now() < node:lastMsgTime + global:repeatErrorDelay
        retu
    endif
    
    node:lastMsg     := msg
//? ' SEGUNDA [' , left(msg,20), '][',left(node:lastMsg,20),']'    
    node:lastMsgTime := now() //int(hb_tton(  hb_DateTime() )*100000)
    
    if msg != ''
	msg:=dtos( date()  )+' '+time()+' ['+node:host+':'+node:port+'/'+node:database+' TX:'+strTrim(thisNode:txid)+' RX:'+strTrim(thisNode:rxid)+'] '+msg //+strTran( msg , chr(13),'')
    endif
    
    ? msg
    //OUTERR("ERR:"+msg)
    //OUTSTD("STD:"+msg)
    if callOnError != nil .and. callOnError
	callOnError(msg)
    endif
retu

func callOnError(msg)
    local cmd
    if ( type( 'global:onError') == 'C' ) .and. ( ( cmd := global:onError )!='' )
	cmd := popen( global:onError , "w")
	pwrite( cmd , msg )
	pclose( cmd )
    endif
retu

exit func _onExit_()
    onExit()
retu

func onExit()
    static yapaso := .f.
    if yapaso
	retu yapaso := .t.
    endif
    if param(1)!=nil
	debug( 'Termino de servicio para el maestro '+param(1) ,.t.,nil )
    endif
retu    

func getLastDateTime(regLifeTime)
    local ret := date()-int( regLifeTime/60/24 )
    local now := explode( time() , ':' )
    local dm  := 60 * 24 * ( regLifeTime/60/24 - int(regLifeTime/60/24) )
    now       := val(now[1])*60 + val(now[2])
    if now < dm
        now+=1440
        ret --
    endif
    now -= dm
retu dtoc( ret ) + ' ' + transform(  int(now/60) , '@0 99' )+':'+transform( ((now/60)-int(now/60))*60-1 , '@0 99' )

func processSignalInfo(node)
    if lastSignal == SIGALRM	//Solicita informacion.
	if node != nil		//Llamado desde bloque interior (replicando registro).
	    debug('Replicando a ['+node:text+':'+node:port+'/'+node:database+'] TX['+strTrim(thisNode:txid)+'] RX:['+strTrim(thisNode:rxid)+']' )
	else			//Llamado desde fuera del ciclo (sin registro por replicar).
	    debug('IDLE')	
	endif
	lastSignal := 0	//Lo callamos
	debug(nil)
    elseif lastSignal $ {SIGINT,SIGQUIT,SIGABRT,SIGKILL,SIGTERM,SIGSTOP}
	quit
    endif
retu .t.

func processSignalHUP()
    if lastSignal == SIGHUP
	reloadConfig()
	lastSignal := 0
	debug('HUP')
	debug(nil)	//reiniciamos el buffer del debug...
			//normalmente si se repite el mismo mensaje de error debug() no lo envia, 
			//por lo que, si arreglamos el problema pero inmediatamente despues se repite el problema
			//el sistema no nos notificara, por lo tanto debemos reiniciarlo...
    endif
retu .t.

func reloadConfig()
    local config_,global_
    if (config_:=getConfig())!=nil .and. (global_:=getGlobalConfig(config_))!=nil
    	global:=global_
    endif
retu

func getConfig()
    static fileName
    if fileName == nil
	if empty(param(2))
	    filename := exename()+'.conf'
	else
	    fileName := param(2)
	endif
    endif
    
    if !file(fileName)
	? "No se encuentra archivo de configuracion ["+fileName+"]"
	?
	__quit()
    endif
    
retu xml2obj2(fileName)

/*extrae parametros globales desde la configuracion (XML)*/
func getGlobalConfig(config)
    local global:=config:getNodeByName:eval('global')

    if mapGet( global , 'USERNAME' )==nil
	debug( 'Error al procesar archivo de configuracion: USERNAME no especificado' )
	retu debug( '' )
    endif
    
    //intervalo de barrido de replicacion en segundos
    global:interval	:= val(  mapGet( global , 'INTERVAL', '1' ) )
    
    //en caso de error por defecto registramos en exename().log
    global:onError	:= mapGet( global , 'ONERROR', [ >> ] + exename() + [.log] )
    
    //tiempo de vida del registro de replicacion (segundos), -1 = inmortal
    global:reglifetime	:= val(mapGet( global , 'REG_LIFETIME', strTrim(REG_NODELETE) ))
    
    //en conexiones fallidas reintentar conexion a los N segundos
    global:connection_retry_delay:=val( mapget(global , 'CONNECTION_RETRY_DELAY','10' ) )

    //tiempo de espera antes de notificar una transaccion en espera
    global:transaction_timeout:=val( mapget(global , 'TRANSACTION_TIMEOUT','600' ) )
    
    //segundos tras los cuales un mensaje se puede repetir
    //si el programa reconoce que el ultimo mensaje de error se esta repitiendo entonces lo omite hasta trascurrir este tiempo...
    global:repeatErrorDelay := val( mapget(global , 'REPEAT_ERROR_DELAY','600' ) )
    
    global:sweep_interval	:= val( mapget(global , 'SWEEP_INTERVAL','600' ) )
    
    global:next_sweep := 0
retu global

func now()
retu   ( date() - stod('19700101') ) * 86400  + int(seconds() )

func setSignal(x)
    static bl
    if x != nil
	bl := x
    endif
retu bl

func mapGet(x,y,z)	; retu hb_HGetDef(x,y,z)
//func sleep(x)		; retu HB_IDLESLEEP(x)
func sysCmd(w,x,y,z)	; retu cmd(w,x,y,z)
func param(x)		; retu HB_ARGV(x)

func xml2obj2(file)
	local ret := map(),o
	local doc := TXmlDocument():New( memoRead( file ) , HBXML_STYLE_NOESCAPE )
	node := TXmlIterator():New( doc:findFirst() )
	ret[0]:={"NODES"=>{}}

	while .t.
	    aAdd( ret[ node:getNode():depth()-1 ]:nodes ,  ret[ node:getNode():depth() ] := xml2obj_newNode(node) )
	    if node:next() == nil

		exit
	    endif
	enddo
	
retu ret[0]:nodes[1]

func xml2obj_newNode(node)
    local obj
    obj := { "NAME" =>upper( node:getNode():cName ), ;
	     "NODES"=>{} , ;
	     "GETNODEBYNAME"=>{|name|
				local itm
				name = upper( name )
				for each itm in obj:nodes
				    if itm:name == name
					retu itm
				    endif
				next
				},;
	     "GETNODESBYNAME"=>{|name|
				local itm,ret:={}
				name := upper( name )
			
				for each itm in obj:nodes
 				    if itm:name == name
					aadd( ret , itm )
				    endif
				next
				
				retu ret
				} }
				
    hb_hEval( node:getNode():aAttributes , {|key,val| obj[ upper( key ) ] := val } )
	
retu obj

func ConnectNew( type, host , port, user_name , password, database, opts )
retu {"CONN"=>TPQServer():new( ;
        	    host ,;
		    database,;
		    user_name,;
		    password,;
		    val(port),,;
		    opts),;
	    "HOST"=>,;
	    "PORT"=>}

func signalHandler(x)
    if !setSignal():eval(x) 
	onExit()
	__quit()
    endif
retu


func initialize()
    public CRLF := chr(13)+chr(10)   
    public global	//public porque hay un type('global:...) mas adelante ...
    public thisNode := nil

    set(_SET_DATEFORMAT,'yyyy.mm.dd')
    hb_setEnv('LANG','es_ES')
    set exac on
    setSignalsHandler()    
    setSignal( {|x| lastSignal:=x , .t. } ) 
retu

								
#pragma begindump

#include <signal.h>
#include "libpq-fe.h"

#include <hbapi.h>
#include <hbvm.h>
void SignalHandler( int sig )
{
  hb_vmPushSymbol( hb_dynsymGetSymbol( "SIGNALHANDLER" ) );
  hb_vmPushNil();
  hb_vmPushLong( sig );
  hb_vmFunction( 1 );
}

HB_FUNC( SETSIGNALSHANDLER )
{
  signal( SIGHUP	, SignalHandler );
  signal( SIGALRM	, SignalHandler );
  signal( SIGTERM	, SignalHandler );
  signal( SIGINT	, SignalHandler );
  signal( SIGQUIT	, SignalHandler );
  signal( SIGKILL	, SignalHandler );
}


/*
HB_FUNC( PQRESULTERRORFIELD ){
    if( hb_parinfo( 1 ) )
        hb_retc( PQresultErrorField( ( PGresult * ) hb_parptr( 1 ) , hb_parni(2) ) );
}
*/

#pragma enddump