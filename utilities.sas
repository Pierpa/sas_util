/*!
 *    	Utilities
 *
 *    	@author         Pierpaolo Paolini
 *    	@created        07/07/2015
 */

/****************************************************************************************************************/

/**
 *    	Conta il numero di righe in un dataset
 *  
 *		@param  	ds			Dataset sas
 *		@return		nobs		Numero di righe nel dataset 			
 */
%macro nobs(ds=);
	%let DSID=%sysfunc(OPEN(&ds.,IN));
	%let NOBS=%sysfunc(ATTRN(&DSID,NOBS));
	%let RC=%sysfunc(CLOSE(&DSID));
	&NOBS
%mend nobs;

/**
 *    	Restituisce varie informazioni per ogni file presente in una certa cartella
 *  
 *    	@param  	path2file  	  	path assoluto dove sono i file ad esempio in questa forma /path/to/file/
 *		@param		name 			nome del file con estensione va bene anche star.xml per prendere tutti i file
 *		@param 		lib2write      	tabella sas di scrittura			
 */
%macro info_file(path2file=, name= ,lib2write=);
	/*filename di appoggio alla pipe*/
	filename dirList pipe "ls --full-time &path2file.&name.";
	
	data &lib2write..dirList;
		infile dirList length=reclen;
		input file $varying200. reclen;

		permission=scan(file,1,"");
		data = INPUT(scan(file, -4, ""), yymmdd10.);
		ora = INPUT(scan(file, -3, ""), time5.);
		path_file = scan(file, -1, "");
		nome = scan(path_file, -1, "/");
		plate = scan(nome,1,"_");
		format data date9.  ora hhmm.;
		drop permission file;
	run;
	
	filename _all_ clear;
%mend info_file;

/**
 *    	Restituisce varie informazioni riguardanti una certa cartella di hadoop
 *  
 *    	@param  	path_hadoop  	path assoluto di hadoop 
 *		@param 		lib2write      	tabella sas di scrittura			
 */
%macro info_hadoop(path_hadoop=, lib2write=);
	filename dirList pipe "hadoop fs -ls &path_hadoop. | tail -n +2 ";

	data &lib2write..dirList;
		infile dirList length=reclen;
		input file $varying200. reclen;

		permission=scan(file,1,"");
		data = INPUT(scan(file, 6, ""), yymmdd10.);
		ora = INPUT(scan(file, 7, ""), time5.);
		path_hadoop = scan(file, 8, "");
		nome = scan(path_hadoop, -1, "/");

		format data date9.  ora hhmm.;
		drop permission file;
	run;
	filename _all_ clear;
%mend info_hadoop;


/**
 *    	legge tutti i file xml 
 *  
 *    	@param  	table2read  	tabella sas da leggere
 *		@param 		table2write		tabella sas in scrittura
 *		@param 		path2map_xml    path assoluto della mappa xml
 *		@param		map_table2read	tabella da leggere nella mappa xml	
 */
%macro readXml(table2read=, table2write=, path2map_xml=, map_table2read=);
	
	%do i=1 %to %nobs(&table2read.);
		
		data _null_;
			set  &table2read.(Firstobs=&i. OBS=&i.);
			call symputx('tmp', path_file);
			call symputx('plate', plate);
		run;

		filename  path2file "&tmp";
		/* Create response xml map file; */
		filename  map_xml "&path2map_xml.";
		libname path2file xmlv2 xmlmap=map_xml;

		%if %sysfunc(exist(&table2write.)) %then %do;
			data tmpAux;
				length plate $8.;
				set path2file.&map_table2read.;
				plate = symget('plate');
			run;

			proc append base=&table2write. data=tmpAux ; run;

		%end;
		%else %do;
			data &table2write.;
			length plate $8.;
				set path2file.&map_table2read.;
				plate = symget('plate');
			run;
		%end;
		libname path2file clear;
		filename _all_ clear;

	%end;
%mend readXml;