#include "RelationalAccess/ISession.h"
#include "RelationalAccess/ISessionProperties.h"
#include "CondTools/RPC/interface/RPCDBCom.h"
#include "RelationalAccess/ITransaction.h"
#include "RelationalAccess/ITypeConverter.h"
#include "RelationalAccess/ISchema.h"
#include "RelationalAccess/ITable.h"
#include "RelationalAccess/ITableDataEditor.h"
#include "RelationalAccess/TableDescription.h"
#include "RelationalAccess/IQuery.h"
#include "RelationalAccess/ICursor.h"
#include "CoralBase/TimeStamp.h"
#include "CoralBase/TimeStamp.h"
#include "CoralBase/Exception.h"
#include "CoralBase/AttributeList.h"
#include "CoralBase/Attribute.h"
#include "CoralBase/AttributeSpecification.h"
#include <iostream>
#include <stdexcept>
#include <vector>
#include <math.h>
#include <fstream>
#include <iostream>
#include <sstream>
#include <time.h>
#include "CondFormats/RPCObjects/interface/RPCObFebmap.h"
#include "CondFormats/RPCObjects/interface/RPCObPVSSmap.h"
#include "CoralKernel/Context.h"
#include "CoralKernel/Context.h"
#include "RelationalAccess/IRelationalDomain.h"
#include "RelationalAccess/IConnection.h"


using namespace std;

  struct pvssmap {
    std::vector<float> dpids;
    std::map<float, std::vector< float > > values;
    std::map<float, std::vector< coral::TimeStamp> > times;
    std::map< long long int, float > is_rpcready;
    std::map< long long int, float > is_cmsrpcready;
  } ;

  struct pvsstablecontent {
    std::vector<float> dpids;
    std::vector<std::string> columns;
    std::map<std::string, map< float, std::vector< float > > > values;
  } ;


int main (int argc,  char* argv[] ) {


  coral::ISession* connect( const std::string& connectionString,
			    const std::string& userName,
			    const std::string& password );

  coral::IConnection* returnConnection( const std::string& connectionString,
			    const std::string& userName,
			    const std::string& password );

  coral::ITypeConverter* GetTypeConverter( const std::string& connectionString,
					   const std::string& userName,
					   const std::string& password );

  coral::TimeStamp UTtoT(long long utime);
    
  std::string returnSchema(const std::string& connectionString);
  
  std::string sqlType(const std::string& column);
  
  std::string cppType(const std::string& column);
  
  stringstream s_firstrun_,s_lastrun_,s_variables_,s_credentials_,s_write_out;
  s_firstrun_  << argv[1];
  s_lastrun_   << argv[2];
  s_variables_ << argv[3];
  s_credentials_ << argv[4];
  s_write_out<< argv[5];

  bool doWriteOutput= 0; 
  s_write_out >> doWriteOutput;
  //Usage  RPCGetPVSSPerRun firstrun lastrun Variables
  //Variables: "I" for Imon, "V" for Vmon, "T"for temperature 
  //NOTE: Statuses are different! if you add the status, the table will add the corresponding column with the info with status = 1;
  // example:  RPCGetPVSSPerRun 200992 2000993  IVS_FW 'credentials.txt' 0 --> will get Imon,Vmon and the StatusB, 
  //and have the column of Imon with status ==1, while getting credentials from the credentials.txt file, and not writing in the output table.
  // RPCGetPVSSPerRun 200992 2000993  IVS_FWTS_ADC credentials.txt 0 will add the temperature from FWCAENADC and the Status of the FWCAENADC. 
  
  std::cout  << s_firstrun_.str() << s_lastrun_.str() << s_variables_.str()<<std::endl; 
  string s_variables = s_variables_.str();
  //  s_variables = " " +s_variables;
  
  cout << "s_variables "<< s_variables <<endl;
  long long firstrun,lastrun;

  s_firstrun_>>firstrun;
  s_lastrun_>>lastrun;

  string I("I"),V("V"),T("T"),S("S_FW"),S_adc("S_ADC");

  


  std::vector<std::string> ivt, ivtOut, ivtTxts, tableNames, supplyTypes;
 
  std::map<string, bool >add_status_info;//,add_status_adc_info;
  std::map <string, pvsstablecontent > tablesContent;
  
  //Important: to save the status appropriate to each table we can conveniently use the order of the table filling. Do NOT change the order!
  //Block 1: do FWCAENCHANNEL: 
  if(s_variables.find(I)!=std::string::npos){ivt.push_back("ACTUAL_IMON");ivtTxts.push_back("Imon");tableNames.push_back("FWCAENCHANNEL");add_status_info["FWCAENCHANNEL"]=false;supplyTypes.push_back("HVLV");}
  if(s_variables.find(V)!=std::string::npos){ivt.push_back("ACTUAL_VMON");ivtTxts.push_back("Vmon");tableNames.push_back("FWCAENCHANNEL");add_status_info["FWCAENCHANNEL"]=false;supplyTypes.push_back("HVLV");}
  
  if(s_variables.find(I)!=std::string::npos && s_variables.find(V)!=std::string::npos){
    ivtOut.push_back("RPCPVSSPERRUN");
  }

  //Block 2: do FWCAENCHANNELADC:
  if(s_variables.find(S_adc)!=std::string::npos){ivt.push_back("DPE_STATUS");ivtTxts.push_back("Stat_ADC");tableNames.push_back("FWCAENCHANNELADC");add_status_info["FWCAENCHANNELADC"]=false;supplyTypes.push_back("T");}
  if(s_variables.find(T)!=std::string::npos){ivt.push_back("ACTUAL_TEMPERATURE");ivtTxts.push_back("Temp");tableNames.push_back("FWCAENCHANNELADC");add_status_info["FWCAENCHANNELADC"]=false;supplyTypes.push_back("T");}
  if(s_variables.find(S)!=std::string::npos){ivt.push_back("ACTUAL_STATUS");ivtTxts.push_back("Stat");tableNames.push_back("FWCAENCHANNEL");add_status_info["FWCAENCHANNEL"]=true;supplyTypes.push_back("HVLV");}
  
  if(s_variables.find(T)!=std::string::npos ){
    ivtOut.push_back("RPCPVSSTEMPPERRUN");
  }
 
  if(s_variables.find(S)!=std::string::npos){add_status_info["FWCAENCHANNEL"]=true;}

  

  if (add_status_info["FWCAENCHANNEL"]==true ){
    if(!(s_variables.find(I)!=std::string::npos&&s_variables.find(V)!=std::string::npos))cout << "WARNING! EITHER IMON OR VMON MISSING! RE-RUN WIHT 'IV' as argument!" << endl;
  }

  //Chceck to add status info:
  

  pvssmap GetTimesAndValues(long long , string, string , string, string);
  string AssociatedOutTable(string);
  void FillOutTable(string, string, pvsstablecontent&, float, float, float, float, float, float, float);
  void InitOutTable(string, string, pvsstablecontent&);
  
  std::map<float, std::vector< float > > dpids_statuses;
  std::map<float, std::vector< coral::TimeStamp> > dpids_statuses_times ;

  std::map<float, std::vector< float > > dpids_statuses_adc;
  std::map<float, std::vector< coral::TimeStamp> > dpids_statuses_adc_times ;

  std::string credential = "credentials.txt";
  if ( s_credentials_.str() !=  "")    credential = s_credentials_.str();
  cout<< " credential "<<credential<<endl;

  ifstream* outputf = new std::ifstream(credential);
  
  std::string m_connectionStringOut;
  std::string m_userNameOut;
  std::string m_passwordOut;
  std::string dummys;
  
  *outputf >> dummys;  *outputf >> dummys;  *outputf >> dummys;
  *outputf >> dummys;  *outputf >> dummys;  *outputf >> dummys;
  *outputf >> m_connectionStringOut;
  *outputf >> m_userNameOut;
  *outputf >> m_passwordOut;  
  
  
  for (long long int run = firstrun; run <=lastrun; ++run){
    
    for(size_t var = 0; var < ivt.size();++var){
      

      std::string variable = ivt.at(var);
      std::string supplyType = supplyTypes.at(var);
      std::string variableTxt = ivtTxts.at(var);
      std::string tableName = tableNames.at(var);
      std::cout << " ivt #"<< var+1 << "  "<< ivt.at(var) <<std::endl;
      
      

      std::cout << ">>processing runs from  "<< firstrun << " to "<< lastrun << std::endl;
    
      
      pvssmap PVSS_MAP = GetTimesAndValues(run, supplyType, variable, tableName , credential );
      string OUT_TABLE = AssociatedOutTable(variable);
      
      InitOutTable(variable,OUT_TABLE,tablesContent[OUT_TABLE]);
      cout << " col size is zzz "<< tablesContent[OUT_TABLE].columns.size()<<endl;
      tablesContent[OUT_TABLE].dpids=PVSS_MAP.dpids; 

    
    
      std::vector<float> dpids =     PVSS_MAP.dpids;
      std::map<float, std::vector< float > > dpids_values =   PVSS_MAP.values;
      std::map<float, std::vector< coral::TimeStamp> > dpids_times =  PVSS_MAP.times ;
      std::map< long long int, float > is_rpcready = PVSS_MAP.is_rpcready;;
      std::map< long long int, float > is_cmsrpcready = PVSS_MAP.is_cmsrpcready;
      
      if (variable.find("STATUS")!=std::string::npos){
	 dpids_statuses = dpids_values;
	 dpids_statuses_times = dpids_times;
      }

      
      std::stringstream ss;
      ss<< run<< "_"+variableTxt+"PerRun.txt";
      ofstream* output = new std::ofstream(ss.str());
      *output << run << std::endl;
      
      
      //      bool empty = false;

      double valmean=0,valmeanrpcready=0,valmeancmsrpcready=0,valmeanisstatusone=0,valmeanisstatusonenofirstpart=0;
      
      for (size_t id_ind = 0;id_ind< dpids.size() ;++id_ind){
	//	std::cout <<  " loop on the averages "<< id_ind <<std::endl;
	valmean =0;
	double value=0, valuerpcready=0, valuecmsrpcready=0,valueisstatusone=0,valueisstatusonenofirstpart=0;
	//long int total_time_check=stop.total_nanoseconds()-start.total_nanoseconds();
	long int total_time=0;
	long int total_timerpcready=0;
	long int total_timecmsrpcready=0;
	long int total_timeisstatusone=0;
	long int total_timeisstatusonenofirstpart=0;
	float did = dpids.at(id_ind);

	bool foundstatus=false;
	bool allstatusone=true;
	bool hastrip=false;
	bool hasovc=false;
	
	for (size_t it_values =0;it_values<dpids_values[did].size();++it_values){
	  long int timeinit = dpids_times[did].at(it_values).total_nanoseconds(); 
	  long int timeofchange = dpids_times[did].at(it_values+1).total_nanoseconds(); 
	  bool isstatusone = false;
	  bool isfirstpart = (dpids_times[did].at(it_values).total_nanoseconds()-dpids_times[did].at(0).total_nanoseconds())<(60.*10.*1000000000.);
	  if ((variable.find("STATUS")==std::string::npos) && add_status_info[tableName] ){	  
	    if((dpids_statuses[did].size()!=0)){
	      if((dpids_statuses[did].size()!=0)){
		for (size_t it_statuses =0;it_statuses<dpids_statuses[did].size();++it_statuses){
		  long int statustime = dpids_statuses_times[did].at(it_statuses).total_nanoseconds(); 
		  if(statustime <= timeinit ){
		    foundstatus = true;
		    isstatusone = (dpids_statuses[did].at(it_statuses)==1);
		  }
		  
		  if(statustime > timeinit && statustime<timeofchange ){
		    foundstatus = true;
		    isstatusone = (isstatusone && (dpids_statuses[did].at(it_statuses)==1));
		    hastrip = ( hastrip || (dpids_statuses[did].at(it_statuses)==9));
		    hasovc = ( hasovc || (dpids_statuses[did].at(it_statuses)==512));

		  }
		}
	      }
	    }
	  }
	  allstatusone = allstatusone && isstatusone;
	  cout << "in the end, is status one?"<< isstatusone << " did I find the status beforehand? "<< foundstatus<< endl;

	  long int time = dpids_times[did].at(it_values+1).total_nanoseconds() - dpids_times[did].at(it_values).total_nanoseconds(); 
	  valmean += dpids_values[did].at(it_values)*time;
	  total_time +=time;
	  
	  
	  std::cout<< " did " << did  <<" @time " << (dpids_times[did].at(it_values)).total_nanoseconds() <<" isrpcready "<< is_rpcready[(dpids_times[did].at(it_values)).total_nanoseconds()]<< " is cms rpc ready "<< is_cmsrpcready[(dpids_times[did].at(it_values)).total_nanoseconds()]<< " " << std::endl; 
	  if(is_rpcready[(dpids_times[did].at(it_values)).total_nanoseconds()]>0.){
	    total_timerpcready+=time;
	    valmeanrpcready+=dpids_values[did].at(it_values)*time;
	  }
	  
	  if(is_cmsrpcready[(dpids_times[did].at(it_values)).total_nanoseconds()]>0.){
	    total_timecmsrpcready+=time;
	    valmeancmsrpcready+=dpids_values[did].at(it_values)*time;
	    if(isstatusone){
	      total_timeisstatusone+=time;
	      valmeanisstatusone+=dpids_values[did].at(it_values)*time;
	      if(!isfirstpart){
		total_timeisstatusonenofirstpart+=time;
		valmeanisstatusonenofirstpart+=dpids_values[did].at(it_values)*time;
	      }
	    }
	  }
	  
	  //	  std::cout << " value change "<< dpids_values[did].at(it_values)<< " time init "<< timeinit<< " time of change "<< timeofchange << " deltatime in secs. "<< time/1000000000. <<std::endl;
	  //std::cout << " valformean "<< dpids_values[did].at(it_values)*time;
	}
	
	//	std::cout << " dpid "<< did <<" valmean " <<valmean <<std::endl;
	//std::cout << "check lumi "<< nlum << " time 1 "<< total_time_check<< " time 2 "<< total_time << std::endl;
	if (total_time>0)valmean = valmean/total_time;
	else valmean = -1;

	if(total_timerpcready >0)valmeanrpcready = valmeanrpcready/total_timerpcready;
	else valmeanrpcready =-1;
	
	if(total_timecmsrpcready >0)valmeancmsrpcready = valmeancmsrpcready/total_timecmsrpcready;
	else valmeancmsrpcready =-1;
	
	if(total_timeisstatusone >0)valmeanisstatusone = valmeanisstatusone/total_timeisstatusone;
	else valmeanisstatusone =-1;

	if(total_timeisstatusonenofirstpart >0)valmeanisstatusonenofirstpart = valmeanisstatusonenofirstpart/total_timeisstatusonenofirstpart;
	else valmeanisstatusonenofirstpart =-1;
	
	//	std::cout << "valmean "<< valmean<<std::endl;
	
      
	int status_info=0;
	if (!foundstatus) status_info=-1;
	if (foundstatus && allstatusone) status_info=1;
	if (foundstatus && !allstatusone) status_info=2;
	if (hastrip) status_info = 9;
	if (hasovc) status_info = 512;

	value=valmean, valuerpcready=valmeanrpcready, valuecmsrpcready=valmeancmsrpcready, valueisstatusone=valmeanisstatusone;  
	valueisstatusonenofirstpart=valmeanisstatusonenofirstpart;  
	
	cout <<run << " " <<did<< " "<< value << " "<<valuerpcready<<" "<< valuecmsrpcready<< " " << valueisstatusone<< " " << valueisstatusonenofirstpart<<" " << status_info<< endl;
	
	*output <<did<< " "<< value << " "<<valuerpcready<<" "<< valuecmsrpcready<< " " << valueisstatusone << " " <<valueisstatusonenofirstpart <<" "<< status_info <<endl;
	cout << " var is "<< variable<< endl;
	FillOutTable(variable,OUT_TABLE,tablesContent[OUT_TABLE], did, value,valuerpcready,valuecmsrpcready,valueisstatusone,valueisstatusonenofirstpart,status_info);
      }
    }
    cout << " vars size "<<  ivtOut.size()<<endl; 
    for(size_t var = 0; var < ivtOut.size();++var){
      cout << " vars #1 "<<  ivtOut.at(var)<<endl; 
    }    

    if(doWriteOutput){
      cout << " writing output to table "<<endl;
      for(size_t var = 0; var < ivtOut.size();++var){
	
	string orignametype = coral::AttributeSpecification::typeNameForId( typeid(float));
	
	
	coral::IConnection* connOut = returnConnection( m_connectionStringOut,
							m_userNameOut, m_passwordOut );
	
	std::string schemaOut  = returnSchema( m_connectionStringOut);
	
	coral::ISession* sessionOut = connOut->newSession( schemaOut );
	if ( sessionOut ) {
	  sessionOut->startUserSession( m_userNameOut, m_passwordOut );
	}
	
	string tableOutName = ivtOut.at(var);
	bool isAlreadyCreated = true;
	sessionOut->transaction().start();
	isAlreadyCreated = sessionOut->nominalSchema().existsTable(tableOutName);
	
	if(!isAlreadyCreated){
	  coral::TableDescription description;
	  description.setName( tableOutName );
	  cout << " creating table " <<tableOutName<< endl;
	  cout << " columns size " <<tablesContent[tableOutName].columns.size()<< endl;
	  
	  string nameint =  sqlType("PVSSID");
	  connOut->typeConverter().setSqlTypeForCppType(nameint, coral::AttributeSpecification::typeNameForId( typeid(int)));
	  description.insertColumn( "PVSSID", coral::AttributeSpecification::typeNameForId( typeid(int) ) );
	  description.insertColumn( "RUN_NUMBER", coral::AttributeSpecification::typeNameForId( typeid(int) ) );
	  description.setNotNullConstraint("PVSSID");
	  description.setNotNullConstraint("RUN_NUMBER");
	  std::vector<string> keys;
	  keys.push_back("PVSSID");
	  keys.push_back("RUN_NUMBER");
	  description.setPrimaryKey(keys);
	  
	  for (size_t c = 0; c <tablesContent[tableOutName].columns.size();++c){
	    cout << "in column loop # " << c<< endl ;
	    string col = tablesContent[tableOutName].columns.at(c);
	    if ((col == "PVSSID") || (col == "RUN_NUMBER")){
	      ;	  }
	    else {
	      string nametype = sqlType(col);
	      string orignametype = cppType(col);
	      connOut->typeConverter().setSqlTypeForCppType(nametype, orignametype);
	      cout<< " origin " <<  orignametype <<" nametype "<< nametype<<endl; 
	      description.insertColumn( col, orignametype);
	      ;}
	    cout << " adding column table " <<col<< endl;
	  }
	  coral::ITable& tableOut = sessionOut->nominalSchema().createTable( description );
	  tableOut = sessionOut->nominalSchema().tableHandle(tableOutName);
	}
	
	std::vector<float> dpids = tablesContent[tableOutName].dpids; 
	coral::ITable& tableOut = sessionOut->nominalSchema().tableHandle(tableOutName);
	
	for (size_t id_ind = 0;id_ind< dpids.size() ;++id_ind){
	  float did = dpids.at(id_ind);
	  float dpid = did; 
	  cout << "before table ins " << endl;
	  cout << " did " <<did<<endl;
	  coral::AttributeList outputRow;
	  //	tableOut.dataEditor().rowBuffer( outputRow );
	  outputRow.extend<int>( "RUN_NUMBER" );
	  outputRow.extend<int>( "PVSSID" );
	  outputRow["PVSSID"].data<int>()= (int)(did);
	  outputRow["RUN_NUMBER"].data<int>()= (int)(run);
	  for (size_t c = 0; c <tablesContent[tableOutName].columns.size();c++){
	    //	  std::vector<string> columns = tablesContent[tableOutName].columns;
	    string col = tablesContent[tableOutName].columns.at(c);
	    outputRow.extend<float>( col );
	    outputRow[col].data<float>()= tablesContent[tableOutName].values[col][dpid].at(tablesContent[tableOutName].values[col][dpid].size()-1);
	    for(size_t sv =0;sv<tablesContent[tableOutName].values[col][did].size();++sv ){ 
	      cout << " column "<< col  << " value " << tablesContent[tableOutName].values[col][did].at(sv)<<endl;
	    }
	    
	  }
	  tableOut.dataEditor().insertRow( outputRow );
	  cout << "after table ins id " << did << endl;
	  
	}	      
	sessionOut->transaction().commit();
      }
    }
  }
}


coral::ITypeConverter* GetTypeConverter( const std::string& connectionString,
					 const std::string& userName,
					 const std::string& password ){
  coral::Context& ctx = coral::Context::instance();
  ctx.loadComponent("CORAL/RelationalPlugins/oracle");
  coral::IHandle<coral::IRelationalDomain> iHandle=ctx.query<coral::IRelationalDomain>("CORAL/RelationalPlugins/oracle");
  if ( ! iHandle.isValid() ) {
    throw std::runtime_error( "Could not load the OracleAccess plugin" );
  }
  std::pair<std::string, std::string> connectionAndSchema = iHandle->decodeUserConnectionString( connectionString );
  
  //  if ( ! m_connection )
  coral::IConnection *m_connection = iHandle->newConnection( connectionAndSchema.first );

if ( ! m_connection->isConnected() )
  m_connection->connect();
  
 coral::ITypeConverter* conv= &m_connection->typeConverter();
 return conv;
}

std::string cppType(const std::string & column){
  string type = "float";
  if (column == "PVSSID" ) type = coral::AttributeSpecification::typeNameForId( typeid(int));
  if (column == "RUN_NUMBER" ) type = coral::AttributeSpecification::typeNameForId( typeid(int));
  if (column == "IMON" ) type = coral::AttributeSpecification::typeNameForId( typeid(float));
  if (column == "IMONRPCON" ) type = coral::AttributeSpecification::typeNameForId( typeid(float));
  if (column == "IMONCMSON" ) type = coral::AttributeSpecification::typeNameForId( typeid(float));
  if (column == "IMONGOODSTATUS" ) type = coral::AttributeSpecification::typeNameForId( typeid(float));
  if (column == "IMONGOODAVERAGE" ) type = coral::AttributeSpecification::typeNameForId( typeid(float));
  if (column == "IMONSTATUS" ) type = coral::AttributeSpecification::typeNameForId( typeid(float));
  if (column == "VMON" ) type = coral::AttributeSpecification::typeNameForId( typeid(long));
  if (column == "VMONRPCON" ) type = coral::AttributeSpecification::typeNameForId( typeid(double));
  if (column == "VMONCMSON" ) type = coral::AttributeSpecification::typeNameForId( typeid(double));
  if (column == "VMONGOODSTATUS" ) type = coral::AttributeSpecification::typeNameForId( typeid(double));
  if (column == "VMONGOODAVERAGE" ) type = coral::AttributeSpecification::typeNameForId( typeid(double));
  if (column == "VMONSTATUS" ) type = coral::AttributeSpecification::typeNameForId( typeid(float));
  return type;
}

std::string sqlType(const std::string & column){
  string type = "NUMBER(10,8)";
  if (column == "PVSSID" ) type = "NUMBER(38)";
  if (column == "RUN_NUMBER" ) type = "NUMBER(38)";
  if (column == "IMON" ) type = "NUMBER(10,8)";
  if (column == "IMONRPCON" ) type = "NUMBER(10,8)";
  if (column == "IMONCMSON" ) type = "NUMBER(10,8)";
  if (column == "IMONGOODSTATUS" ) type = "NUMBER(10,8)";
  if (column == "IMONGOODAVERAGE" ) type = "NUMBER(10,8)";
  if (column == "IMONSTATUS" ) type = "NUMBER(10,8)";
  if (column == "VMON" ) type = "NUMBER(14,8)";
  if (column == "VMONRPCON" ) type = "NUMBER(14,8)";
  if (column == "VMONCMSON" ) type = "NUMBER(14,8)";
  if (column == "VMONGOODSTATUS" ) type = "NUMBER(14,8)";
  if (column == "VMONGOODAVERAGE" ) type = "NUMBER(14,8)";
  if (column == "VMONSTATUS" ) type = "NUMBER(10,8)";
  return type;
}

std::string returnSchema( const std::string& connectionString){
  coral::Context& ctx = coral::Context::instance();
  ctx.loadComponent("CORAL/RelationalPlugins/oracle");
  coral::IHandle<coral::IRelationalDomain> iHandle=ctx.query<coral::IRelationalDomain>("CORAL/RelationalPlugins/oracle");
  if ( ! iHandle.isValid() ) {
    throw std::runtime_error( "Could not load the OracleAccess plugin" );
  }
  std::pair<std::string, std::string> connectionAndSchema = iHandle->decodeUserConnectionString( connectionString );
  return connectionAndSchema.second;
}


coral::IConnection* returnConnection( const std::string& connectionString,
			  const std::string& userName,
			  const std::string& password ){

  coral::Context& ctx = coral::Context::instance();
  ctx.loadComponent("CORAL/RelationalPlugins/oracle");
  coral::IHandle<coral::IRelationalDomain> iHandle=ctx.query<coral::IRelationalDomain>("CORAL/RelationalPlugins/oracle");
  if ( ! iHandle.isValid() ) {
    throw std::runtime_error( "Could not load the OracleAccess plugin" );
  }
  std::pair<std::string, std::string> connectionAndSchema = iHandle->decodeUserConnectionString( connectionString );
  
  coral::IConnection *m_connection = iHandle->newConnection( connectionAndSchema.first );
  
  if ( ! m_connection->isConnected() )   m_connection->connect();
  
  return m_connection;
}


coral::ISession* connect( const std::string& connectionString,
			  const std::string& userName,
			  const std::string& password )
{
  coral::Context& ctx = coral::Context::instance();
  ctx.loadComponent("CORAL/RelationalPlugins/oracle");
  coral::IHandle<coral::IRelationalDomain> iHandle=ctx.query<coral::IRelationalDomain>("CORAL/RelationalPlugins/oracle");
  if ( ! iHandle.isValid() ) {
    throw std::runtime_error( "Could not load the OracleAccess plugin" );
  }
  std::pair<std::string, std::string> connectionAndSchema = iHandle->decodeUserConnectionString( connectionString );
  
  //  if ( ! m_connection )
  coral::IConnection *m_connection = iHandle->newConnection( connectionAndSchema.first );
  
  if ( ! m_connection->isConnected() )
    m_connection->connect();

  coral::ISession* session = m_connection->newSession( connectionAndSchema.second );
  
  if ( session ) {
    session->startUserSession( userName, password );
  }
  //memory leaking
  return session;
}


coral::TimeStamp UTtoT(long long utime) 
{
  
  
  int yea = static_cast<int>(trunc(utime/31536000) + 1970);
  int yes = (yea-1970)*31536000;
  int cony = ((yea-1972)%4)+1;
  if (cony == 0) yes = yes + (yea-1972)/4*86400; 
  else yes = yes +  static_cast<int>(trunc((yea-1972)/4))*86400;
  int day = static_cast<int>(trunc((utime - yes)/86400));
  int rest = static_cast<int>(utime - yes - day*86400);
  int mon = 0;
  // BISESTILE YEAR
  if (cony == 0) {
    day = day + 1; 
    if (day < 32){
      mon = 1;
      day = day - 0;
    }
    if (day >= 32 && day < 61){
      mon = 2;
      day = day - 31;
    }
    if (day >= 61 && day < 92){
      mon = 3;
      day = day - 60;
    }
    if (day >= 92 && day < 122){
      mon = 4;
      day = day - 91;
    }
    if (day >= 122 && day < 153){
      mon = 5;
      day = day - 121;
    }
    if (day >= 153 && day < 183){
      mon = 6;
      day = day - 152;
    }
    if (day >= 183 && day < 214){
      mon = 7;
      day = day - 182;
    }
    if (day >= 214 && day < 245){
      mon = 8;
      day = day - 213;
    }
    if (day >= 245 && day < 275){
      mon = 9;
      day = day - 244;
    }
    if (day >= 275 && day < 306){
      mon = 10;
      day = day - 274;
    }
    if (day >= 306 && day < 336){
      mon = 11;
      day = day - 305;
    }
    if (day >= 336){
      mon = 12;
      day = day - 335;
    }
  }
  // NOT BISESTILE YEAR
  else {
    if (day < 32){
      mon = 1;   
      day = day - 0;
    }
    if (day >= 32 && day < 60){
      mon = 2;
      day = day - 31;
    }
    if (day >= 60 && day < 91){
      mon = 3;
      day = day - 59;
    }
    if (day >= 91 && day < 121){
      mon = 4;
      day = day - 90;
    }
    if (day >= 121 && day < 152){
      mon = 5;
      day = day - 120;
    }
    if (day >= 152 && day < 182){
      mon = 6;
      day = day - 151;
    }
    if (day >= 182 && day < 213){
      mon = 7;
      day = day - 181;
    }
    if (day >= 213 && day < 244){
      mon = 8;
      day = day - 212;
    }
    if (day >= 244 && day < 274){
      mon = 9;
      day = day - 243;
    }
    if (day >= 274 && day < 305){
      mon = 10;
      day = day - 273;
    }
    if (day >= 305 && day < 335){
      mon = 11;
      day = day - 304;
    }
    if (day >= 335){
      mon = 12;
      day = day - 334;
    }
  }
  
  int hou = static_cast<int>(trunc(rest/3600)); 
  rest = rest - hou*3600;
  int min = static_cast<int>(trunc(rest/60));
  rest = rest - min*60;
  int sec = rest; 
  int nan = 0;

  //  std::cout <<">> Processing since: "<<day<<"/"<<mon<<"/"<<yea<<" "<<hou<<":"<<min<<"."<<sec<< std::endl;

  coral::TimeStamp Tthr;  

  Tthr = coral::TimeStamp(yea, mon, day, hou, min, sec, nan);
  return Tthr;
}

void InitOutTable (string inputTable, string outputTable, pvsstablecontent & contentToFill){
  if (inputTable == "ACTUAL_IMON"){
    contentToFill.columns.push_back("IMON");
    contentToFill.columns.push_back("IMONRPCON");
    contentToFill.columns.push_back("IMONCMSON");
    contentToFill.columns.push_back("IMONGOODSTATUS");
    contentToFill.columns.push_back("IMONGOODAVERAGE");
    contentToFill.columns.push_back("IMONSTATUS");
  }
  if (inputTable == "ACTUAL_VMON"){
    contentToFill.columns.push_back("VMON");
    contentToFill.columns.push_back("VMONRPCON");
    contentToFill.columns.push_back("VMONCMSON");
    contentToFill.columns.push_back("VMONGOODSTATUS");
    contentToFill.columns.push_back("VMONGOODAVERAGE");
    contentToFill.columns.push_back("VMONSTATUS");
  }

  cout << " col size is zzz "<< contentToFill.columns.size()<<endl;
  return;
}

void FillOutTable(string inputTable, string outputTable, pvsstablecontent & contentToFill, float did, float value, float valuerpcready, float valuecmsready, float valuestatusone, float valuegoodaverage , float status){
  if (inputTable == "ACTUAL_IMON"){
    contentToFill.values["IMON"][did].push_back(value);
    contentToFill.values["IMONRPCON"][did].push_back(valuerpcready);
    contentToFill.values["IMONCMSON"][did].push_back(valuecmsready);
    contentToFill.values["IMONGOODSTATUS"][did].push_back(valuestatusone);
    contentToFill.values["IMONGOODAVERAGE"][did].push_back(valuegoodaverage);
    contentToFill.values["IMONSTATUS"][did].push_back(valuegoodaverage);
    std::cout << "filled did " << did << "value " <<contentToFill.values["IMON"][did].at(contentToFill.values["IMON"][did].size()-1)<<endl;
  }


  if (inputTable == "ACTUAL_VMON"){
    contentToFill.values["VMON"][did].push_back(value);
    contentToFill.values["VMONRPCON"][did].push_back(valuerpcready);
    contentToFill.values["VMONCMSON"][did].push_back(valuecmsready);
    contentToFill.values["VMONGOODSTATUS"][did].push_back(valuestatusone);
    contentToFill.values["VMONGOODAVERAGE"][did].push_back(valuegoodaverage);
    contentToFill.values["VMONSTATUS"][did].push_back(valuegoodaverage);
    std::cout << "filled did " << did << "value " <<contentToFill.values["VMON"][did].at(contentToFill.values["VMON"][did].size()-1)<<endl;
  }

  if (inputTable == "ACTUAL_TEMP"){
    contentToFill.values["TEMP"][did].push_back(value);
    contentToFill.values["TEMPRPCON"][did].push_back(valuerpcready);
    contentToFill.values["TEMPCMSON"][did].push_back(valuecmsready);
    contentToFill.values["TEMPGOODSTATUS"][did].push_back(valuestatusone);
    contentToFill.values["TEMPGOODAVERAGE"][did].push_back(valuegoodaverage);
    contentToFill.values["TEMPSTATUS"][did].push_back(valuegoodaverage);
    std::cout << "filled did " << did << "value " <<contentToFill.values["TEMP"][did].at(contentToFill.values["TEMP"][did].size()-1)<<endl;
  }

return;

}

string AssociatedOutTable(string inName){
  string outname = "" ;
  if (inName == "ACTUAL_IMON"){
    outname = "RPCPVSSPERRUN";
  }
  if (inName == "ACTUAL_VMON"){
    outname = "RPCPVSSPERRUN";
  }
  if (inName == "ACTUAL_TEMP"){
    outname = "RPCPVSSTEMPPERRUN";
  }
  
  return outname; 
}


pvssmap GetTimesAndValues(long long runzero, string supplyType, string variable, string tableName, string filein ){
  
  std::string m_connectionString;
  std::string m_userName;
  std::string m_password;
  std::string m_connectionStringPvssInfo;
  std::string m_userNamePvssInfo;
  std::string m_passwordPvssInfo;
  std::string m_connectionStringRunInfo;
  std::string m_userNameRunInfo;
  std::string m_passwordRunInfo;
  

  ifstream* input = new std::ifstream(filein);

  m_connectionString =  "";
  m_userName="";
  m_password="";
  m_connectionStringRunInfo="";
  m_userNameRunInfo="";
  m_passwordRunInfo="";
  m_connectionStringPvssInfo="";
  m_userNamePvssInfo="";
  m_passwordPvssInfo="";

  
  *input >> m_connectionString;
  *input >> m_userName;
  *input >> m_password;

  *input >> m_connectionStringRunInfo;
  *input >> m_userNameRunInfo;
  *input >> m_passwordRunInfo;


  
  string tableSupply = "RPCPVSSDETID";
  std::vector<string> supplyToQuery;
  if (supplyType.find("HVLV")!= string::npos) {
    supplyToQuery.push_back("HV");
    supplyToQuery.push_back("LVA");  
    supplyToQuery.push_back("LVD");  
  }
  if (supplyType.find("T")!= string::npos) {
    supplyToQuery.push_back("T");  
  }
  string supplyQuery= "RPCPVSSDETID.PVSS_ID is not NULL AND ";
  for (size_t su = 0; su <supplyToQuery.size();++su){
    supplyQuery = supplyQuery + tableSupply+".SUPPLYTYPE = '" + supplyToQuery.at(su)+"'";
    if (su != supplyToQuery.size()-1) supplyQuery = supplyQuery+(" OR "); 
  }

  ///  cout << " supplyQuery is :" << endl << supplyQuery<<endl;

   //   std::map<float, std::vector< float > > GetValues;
   //std::map<float, std::vector< float > > GetTimes;

  pvssmap PVSS_MAP;
   

 
   
    std::vector<float> dpids;
    std::map<float, std::vector< coral::TimeStamp > > dpids_initial_times;
    std::map<float, std::vector< float > > dpids_initial_values;
    //    std::map<float, std::vector< float > > dpids_initial_statuses;
    std::map<float, std::vector< float > > dpids_values;
    //std::map<float, std::vector< float > > dpids_statuses;
    std::map<float, std::vector< coral::TimeStamp> > dpids_times;
    
    coral::ISession* sessionRunInfo = connect( m_connectionStringRunInfo,
					       m_userNameRunInfo, m_passwordRunInfo );
    
    sessionRunInfo->transaction().start( true );
    coral::ISchema& schemaRunInfo = sessionRunInfo->nominalSchema();
    
    coral::ISession* session = connect( m_connectionString,
					      m_userName, m_password );
    
    session->transaction().start( true );
    coral::ISchema& schema = session->nominalSchema();
    
    
    
    
    
    {
      coral::IQuery* queryZero = schema.newQuery();
      
      //New query: for the supplytype
      coral::AttributeList conditionDataSupply;
      queryZero->addToTableList( tableSupply );
      queryZero->addToOutputList( tableSupply+".PVSS_ID ", "PVSS_ID" );
      queryZero->setCondition( supplyQuery + " ORDER BY " + tableSupply+".PVSS_ID ", conditionDataSupply );
      queryZero->setDistinct();
      //      cout << " supplyQuery final is :" << endl <<  (tableSupply+".PVSS_ID "+ supplyQuery)<<endl;      
      
      coral::ICursor& cursorZero = queryZero->execute();
      while ( cursorZero.next() ) {
	const coral::AttributeList& rowZero = cursorZero.currentRow();
	int did  = rowZero["PVSS_ID"].data<int>();
	std::cout<< " did "<< did << std::endl; 
	dpids.push_back(did); //This takes the dpid
      }
    }

    //    dpids.push_back(143494);//This is for tests 
    //    dpids.push_back(37073);
    //    dpids.push_back(9397);

    long long firstrun = runzero, lastrun= runzero;

  for (long long int run = firstrun; run <=lastrun; ++run){

    for (size_t id_ind = 0;id_ind< dpids.size() ;++id_ind){
      float did = dpids.at(id_ind);
      dpids_values[did].clear();
      //      dpids_statuses[did].clear();
      dpids_times[did].clear();
      dpids_initial_values[did].clear();
      //dpids_initial_statuses[did].clear();
      dpids_initial_values[did].push_back(-1.0);
      dpids_initial_times[did].clear();
    }
    
    std::vector<coral::TimeStamp> starttimes;
    std::vector<coral::TimeStamp> stoptimes;
    std::map< long long int, float > is_rpcready;
    std::map< long long int, float > is_cmsrpcready;
    coral::AttributeList conditionDataRun;
    coral::AttributeList conditionDataRunRPC;
    coral::AttributeList conditionDataRunCMSRPC;
    coral::IQuery* queryRunInfo = schemaRunInfo.newQuery();
    //    coral::IQuery* queryRunInfoRPC = schemaRunInfo.newQuery();
    //coral::IQuery* queryRunInfoCMSRPC = schemaRunInfo.newQuery();

    //    coral::IQuery* querySupplyType = schemaRunInfo.newQuery();

    queryRunInfo->addToTableList( "LUMI_SECTIONS" );
    queryRunInfo->addToOutputList( "STARTTIME", "STARTTIME" );
    queryRunInfo->addToOutputList( "STOPTIME", "STOPTIME" );
    queryRunInfo->addToOutputList( "RUNNUMBER", "RUNNUMBER" );
    queryRunInfo->addToOutputList( "LUMISECTION", "LUMISECTION" );

    queryRunInfo->addToOutputList( "RPC_READY", "ISRPCREADY" );
    queryRunInfo->addToOutputList( "CMS_ACTIVE", "ISCMSACTIVE" );
    


    conditionDataRun.extend<long long int>( "run" );
    
    conditionDataRun["run"].data<long long int>() = run;
    std::string conditionRun = "RUNNUMBER = :run"; 
    std::string conditionRunRPC = "RUNNUMBER = :run and RPC_READY > 0 "; 
    std::string conditionRunCMSRPC = "RUNNUMBER = :run and RPC_READY > 0 and CMS_ACTIVE > 0"; 
    queryRunInfo->setCondition( conditionRun , conditionDataRun );

    coral::ICursor& cursorRunInfo = queryRunInfo->execute();
    bool empty = true;
    while ( cursorRunInfo.next() ) {
      empty = false;
      const coral::AttributeList& row = cursorRunInfo.currentRow();
      coral::TimeStamp start =  row["STARTTIME"].data<coral::TimeStamp>();
      coral::TimeStamp stop =  row["STOPTIME"].data<coral::TimeStamp>();
      float isrpcready =  row["ISRPCREADY"].data<bool>();
      float iscmsactive =  row["ISCMSACTIVE"].data<bool>();
      
      is_rpcready[start.total_nanoseconds()]=isrpcready;
      is_cmsrpcready[start.total_nanoseconds()]=iscmsactive * isrpcready;    
      
      starttimes.push_back(start);
      stoptimes.push_back(stop);
    }
    //    std::cout << " empty? "<< empty<< std::endl;
    //Condition on the run:
    
    coral::AttributeList conditionData;
    coral::AttributeList conditionDataZero;
    coral::AttributeList conditionDataPrevious;
    coral::TimeStamp start,stop,previousStart,previousStop;
    conditionData.extend<coral::TimeStamp>( "start" );
    conditionData.extend<coral::TimeStamp>( "stop" );
    conditionDataPrevious.extend<coral::TimeStamp>( "previousstart" );
    conditionDataPrevious.extend<coral::TimeStamp>( "previousstop" );
    
    size_t nlumis = starttimes.size();
    
    if(nlumis >0){
      start = starttimes.at(0);
      stop = stoptimes.at(nlumis-1); 
      previousStart= starttimes.at(0);
      previousStop= starttimes.at(0);
    }
    
    for (size_t id_ind = 0;id_ind< dpids.size() ;++id_ind){
      float did = dpids.at(id_ind);
      dpids_initial_times[did].push_back(start);
    }
    

    conditionData["start"].data<coral::TimeStamp>() = start;
    conditionData["stop"].data<coral::TimeStamp>() = stop;
    
    //Fetch the first condition previous to the time interval:
    std::string condition = tableName+"."+variable+" IS NOT NULL AND "+tableName+".CHANGE_DATE <:start AND "+tableName+".CHANGE_DATE> :previous";

  
    
    size_t count_dpids_with_previous=0;
    
    
    bool isfirst =true;// (run==firstrun );
    
    conditionData["start"].data<coral::TimeStamp>() = start;
    conditionData["stop"].data<coral::TimeStamp>() = stop;
    
    
    conditionDataPrevious["previousstart"].data<coral::TimeStamp>() = previousStart;
    conditionDataPrevious["previousstop"].data<coral::TimeStamp>() = previousStop;
    
    //First part: make sure to have
    bool allDpidsHavePrevious = false;
    
    previousStart = start; 
    previousStop = start; 
    
    int nDaysToLookBack = 0;
    int maxDaysToLookBack = 32; //2;
    
    for (size_t id_ind = 0;id_ind< dpids.size() ;++id_ind){
      float did = dpids.at(id_ind);
      dpids_values[did].clear();
      //	dpids_statuses[did].clear();
      dpids_times[did].clear();
    }
    
    for (size_t id_ind = 0;id_ind< dpids.size() ;++id_ind){
	float did = dpids.at(id_ind);
	if(dpids_initial_values[did].at(0)>=0){
	  dpids_values[did].push_back(dpids_initial_values[did].at(0));
	  ++count_dpids_with_previous;
	}
      }
      allDpidsHavePrevious = (count_dpids_with_previous == dpids.size());
  
      long int DeltaDays=1;
    
      while(allDpidsHavePrevious==false && isfirst  && !empty){

	//std::cout<<" test should I be in this loop"<< !allDpidsHavePrevious << std::endl; 
	long int Day = 86400*1;
	long int nWeekConvStart = Day*(nDaysToLookBack+DeltaDays);
	long int nWeekConvStop = Day*(nDaysToLookBack);
       
	//	std::cout <<"nwconv start " <<  nWeekConvStart<<std::endl;
	//	std::cout <<"nwconv start " <<  nWeekConvStop<<std::endl;
	//	std::cout <<"start " <<  start.total_nanoseconds()/1000000000<<std::endl;
	//	std::cout <<"nwconv UT " << TtoUT(start) <<std::endl;
	//	std::cout<<" double invert "<< (UTtoT(TtoUT(start)+86400)).total_nanoseconds()<<std::endl;
	//	std::cout<<" double invert v2 "<< (UTtoT(start.total_nanoseconds()/1000000000)).total_nanoseconds()<<std::endl;
	
	long int utotstart = start.total_nanoseconds()/1000000000;
	
	previousStart =  UTtoT(utotstart-nWeekConvStart);
	previousStop = UTtoT(utotstart-nWeekConvStop);
	
	conditionDataPrevious["previousstart"].data<coral::TimeStamp>() = previousStart;
	conditionDataPrevious["previousstop"].data<coral::TimeStamp>() = previousStop;
	
	coral::IQuery* queryPrevious = schema.newQuery();
	queryPrevious->addToTableList( tableName );
	queryPrevious->addToOutputList( tableName+".DPID", "DPID" );
	queryPrevious->addToOutputList( tableName+".CHANGE_DATE", "TSTAMP" );
	queryPrevious->addToOutputList( tableName+"."+variable, variable );
	condition = tableName+"."+variable+" IS NOT NULL AND "+tableName+".CHANGE_DATE >:previousstart AND "+tableName+".CHANGE_DATE <:previousstop ORDER BY "+tableName+".CHANGE_DATE";
	queryPrevious->setCondition( condition , conditionDataPrevious );
	coral::ICursor& cursorPrev = queryPrevious->execute();
	
	
	while ( cursorPrev.next() ) {
	  const coral::AttributeList& rowPrev = cursorPrev.currentRow();
	  float did  = rowPrev["DPID"].data<float>();
	  float val = rowPrev[variable].data<float>();
	  coral::TimeStamp ts =  rowPrev["TSTAMP"].data<coral::TimeStamp>();
	  if(!dpids_values[did].size()>0) {
	    if(dpids_initial_times[did].size()>0) {
	      dpids_initial_times[did].at(0) = ts;
	    }
	    else {
	      dpids_initial_times[did].push_back(ts);
	    }
	    
	    if(dpids_initial_values[did].size()>0){
	      dpids_initial_values[did].at(0) = val;
	    }
	    else{
	      dpids_initial_values[did].push_back(val);
	    }
	  }
	}
	
	for (size_t id_ind = 0;id_ind< dpids.size() ;++id_ind){
	  float did = dpids.at(id_ind);
	  if(dpids_values[did].size()>0)continue;
	  
	  if(dpids_initial_values[did].size()!=0){//std::cout << "dpid "<< did << " has init val "<< dpids_initial_values[did].at(0)  <<std::endl ;
;
	    if(dpids_initial_values[did].at(0)>=0){
	      dpids_values[did].push_back(dpids_initial_values[did].at(0));
	      ++count_dpids_with_previous;
	    }
	  }
	}
	allDpidsHavePrevious = (count_dpids_with_previous == dpids.size());
	
	if ( nDaysToLookBack>=maxDaysToLookBack ){
	  break; 
	}
	nDaysToLookBack+=DeltaDays;
	if ( allDpidsHavePrevious == true )break; 
	

	

      }
      //std::cout <<  " I am free "<<std::endl;

      for (size_t id_ind = 0;id_ind< dpids.size() ;++id_ind){
	dpids_times[dpids.at(id_ind)].push_back(start);
      }
      
      
      for(size_t nlum = 0;nlum < nlumis;++nlum){
	
	if (empty) continue;

      	start = starttimes.at(nlum);
	stop = stoptimes.at(nlum);
	

	//std::cout << "check lumi "<<nlum << std::endl;
	
	conditionData["start"].data<coral::TimeStamp>() = start;
	conditionData["stop"].data<coral::TimeStamp>() = stop;
	
	float isrpcready =0.;
	float iscmsrpcready=0.;

	
	isrpcready =  is_rpcready[start.total_nanoseconds()];
	iscmsrpcready =  is_cmsrpcready[start.total_nanoseconds()];
	//std::cout<< " start at time " << start.total_nanoseconds() <<" isrpcready " << is_rpcready[start.total_nanoseconds()]<< std::endl;

	
	coral::IQuery* query = schema.newQuery();
	query->addToTableList( tableName );
	query->addToOutputList( tableName+".DPID", "DPID" );
	query->addToOutputList( tableName+".CHANGE_DATE", "TSTAMP" );
	query->addToOutputList( tableName+"."+variable, variable );
	condition = tableName+"."+variable+" IS NOT NULL AND "+tableName+".CHANGE_DATE >:start AND "+tableName+".CHANGE_DATE <:stop ORDER BY "+tableName+".CHANGE_DATE";
	query->setCondition( condition , conditionData );
	
	coral::ICursor& cursorI = query->execute();
	
	//std::cout <<  " I am about to do the query "<<std::endl;
	//std::cout<< " init time lumi "<< start.total_nanoseconds()/1000000000.<< " end time lumi "<< stop.total_nanoseconds()/1000000000.<<std::endl;

	//Do calculation lumi by lumi
	for (size_t id_ind = 0;id_ind< dpids.size() ;++id_ind){
	  float did = dpids.at(id_ind);
	  if(dpids_values[did].size()>0){ 
	       dpids_values[did].push_back(dpids_values[did].at(dpids_values[did].size()-1));
	       dpids_times[did].push_back(start);
	     }
	}
	
	while ( cursorI.next() ) {
	  const coral::AttributeList& row = cursorI.currentRow();
	  float did = row["DPID"].data<float>();
	  float val = row[variable].data<float>();
	  coral::TimeStamp ts =  row["TSTAMP"].data<coral::TimeStamp>();
	  
	  dpids_values[did].push_back(val);
	  dpids_times[did].push_back(ts);
	  //Itemp.run = (float)run;
	
	  //std::cout<< " did attime " << ts.total_nanoseconds() <<" isrpcready " << isrpcready<< std::endl;
	  is_rpcready[ts.total_nanoseconds()]=isrpcready;
	  is_cmsrpcready[ts.total_nanoseconds()]=iscmsrpcready;
	  
	}

      } 
       
      for (size_t id_ind = 0;id_ind< dpids.size() ;++id_ind){
	dpids_times[dpids.at(id_ind)].push_back(stop);
      
      }
  
            
      PVSS_MAP.dpids = dpids;
      PVSS_MAP.values = dpids_values;
      PVSS_MAP.times = dpids_times;
      PVSS_MAP.is_rpcready = is_rpcready;
      PVSS_MAP.is_cmsrpcready = is_cmsrpcready;
      
  }
  //  long since = 0, till = 1;
  sessionRunInfo->transaction().commit();
  delete sessionRunInfo;

  return PVSS_MAP;
 }


