# -*- coding: utf-8 -*-
"""
Created on Tue Mar 23 17:18:18 2021

@author: phili
"""
import setup_stuff as constants 
import pandas as pd

# ==== NAME OF CONSTANTS ==== # 
# Stoplist_52F_W
# Stoplist_52F_E
# process( ) -- function to handle api call, based on a stop direction. 
# =========================== # 

# ==== DEFINED PARAMETERS ==== # 
base_url = "http://webservices.nextbus.com/service/publicXMLFeed?command=predictions&a=ttc&stopId=" 
end_url = "&r=52"
realtime_data  = pd.DataFrame( columns= constants.column_list )
# =========================== # 


# ==== REQUEST LOOP ==== # 
import time 
from datetime import datetime, date

def request_job(  ): 
    global realtime_data # reference the dataset which will pull 
    
    try: 
        #Prelim logging
        print( "...starting API calls at ", datetime.now() )
        
        # ==================== 52F ===================== # 
        #West first
        for stop in constants.Stoplist_52F_W:
          api_call = base_url + stop + end_url 
          results_df = constants.process( api_call, stop, "52F - West" )
          realtime_data = realtime_data.append( results_df  ) 
    
        #East next
        # 1.0 Make API Request, collect returned xml
        for stop in constants.Stoplist_52F_E:
          api_call = base_url + stop + end_url 
          results_df = constants.process( api_call , stop, "52F - East")
          realtime_data = realtime_data.append( results_df  )
          
        # ==================== 52G ===================== # 
        #West first
        for stop in constants.Stoplist_52G_W:
          api_call = base_url + stop + end_url 
          results_df = constants.process( api_call, stop, "52G - West" )
          realtime_data = realtime_data.append( results_df  ) 
    
        #East next
        # 1.0 Make API Request, collect returned xml
        for stop in constants.Stoplist_52G_E:
          api_call = base_url + stop + end_url 02
          results_df = constants.process( api_call , stop, "52G - East")
          realtime_data = realtime_data.append( results_df  )
       # ==================== 52G ===================== # 
    
        #Output some Stats 
        print( "... New dataset length: ", len( realtime_data.index ) ) 
        
    except Exception as e: 
        print( "An error occured: ")
        print( e.message, e.args ) 
        print( "...saving pulled data to date... ")
        output_string = "realtime_data_" + str( time.time() ) + ".csv"
        realtime_data.to_csv( output_string )
        quit() 
    
    return 

# API CALL LOOP # 
start_time = datetime.now() 
end_script_date = input( "ENTER THE DATE TO END SCRIPT: " ) # ASK FOR RUN_UNTIL_DATE 
print( "running script.... Ending as soon as it's ", end_script_date)
prev_call_date = str( date.today() )
current_date = str( date.today() )

while ( str(date.today())  != end_script_date ) : 
  # UPDATE DATES: 
  prev_call_date = current_date 
  current_date = str( date.today() )

  if( prev_call_date != current_date ): 
    #We've started calling for a new date... export everything
    print( ".... writing the final results to csv for ", prev_call_date  ) # TODO: Change date.today() to prev_call_date
    output_string =  "realtime_data_" + str( prev_call_date ) + ".csv"
    realtime_data.to_csv( output_string ) 

    #Clean and return to calls
    realtime_data = realtime_data.iloc[0:0] #unreference all previous data, keep headers. 

  # RUN CALLS AND LOG 
  request_job( ) # DO request job, previously schedule.run_pending() 
  time.sleep( 15 * 60 ) #sleep for 15 minutes, then repeat

# TIME PERIOD OVER 
end_time = datetime.now() 
print( "API CALLER STARTED @ ", start_time, "   .. and ended @ ", end_time ) 
print( ".... writing the final results to csv " ) 
output_string = "realtime_data_" + str( time.time() ) + ".csv"
realtime_data.to_csv( output_string ) 

"""
# API CALL LOOP # 
start_time = datetime.now() 
end_script_date = input( "ENTER THE DATE TO END SCRIPT: " ) # ASK FOR RUN_UNTIL_DATE 
print( "running script.... Ending as soon as it's ", end_script_date)
schedule.every(15).minutes.do( request_job ) 

while ( str(date.today())  != end_script_date ) : 
    schedule.run_pending() 
    time.sleep( 1 )

# TIME PERIOD OVER 
end_time = datetime.now() 
print( "API CALLER STARTED @ ", start_time, "   .. and ended @ ", end_time ) 
print( ".... writing the final results to csv " ) 
output_string = "realtime_data_" + str( time.time() ) + ".csv"
realtime_data.to_csv( output_string ) 
"""