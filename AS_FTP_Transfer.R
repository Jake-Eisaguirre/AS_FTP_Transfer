tryCatch({
  if (!require(librarian)) {
    install.packages("librarian")
    library(librarian)
  }
  
  # librarian downloads, if not already downloaded, and reads in needed packages
  librarian::shelf(tidyverse, here, DBI, odbc, rstudioapi)
  
  # Connect to the `PLAYGROUND` database and append data if necessary
  db_connection_pg <- DBI::dbConnect(odbc::odbc(),
                                     Driver = "SnowflakeDSIIDriver",
                                     Server = "hawaiianair.west-us-2.azure.snowflakecomputing.com",
                                     WAREHOUSE = "DATA_LAKE_READER",
                                     Database = "PLAYGROUND",
                                     UID = "jacob.eisaguirre@hawaiianair.com",
                                     authenticator = "externalbrowser")
  print("Database Connected!")
  
  # Set schema and retrieve data from `AA_FINAL_PAIRING` table
  dbExecute(db_connection_pg, "USE SCHEMA CREW_ANALYTICS")
  
  OPL_q <- "
            SELECT 
                'HA' AS AIRLINE,
                COALESCE(r.EQUIPMENT, s.EQUIPMENT) AS FLEETTYPE,
                COALESCE(r.DATE, s.DATE) AS DATE,
                CASE 
                    WHEN COALESCE(r.EQUIPMENT, s.EQUIPMENT) = '717' THEN 'HNL'
                    ELSE COALESCE(r.BASE, s.BASE)
                END AS BASE,
                COALESCE(r.PAIRING_POSITION, s.PAIRING_POSITION) AS SEAT,
                COALESCE(r.NUM_RESERVE, 0) AS AVAILABLE_RESERVES,
                COALESCE(s.NUM_SICK, 0) AS SICK_LEAVE
            FROM (
                -- Query for Reserve employees
                SELECT 
                    CASE 
                        WHEN PAIRING_POSITION = 'RO' THEN 'FO' 
                        WHEN PAIRING_POSITION IN ('FO', 'CA', 'FA') THEN PAIRING_POSITION 
                        ELSE 'FA' 
                    END AS PAIRING_POSITION, 
                    BASE, 
                    PAIRING_DATE AS DATE, 
                    EQUIPMENT, 
                    COUNT(DISTINCT CREW_ID) AS NUM_RESERVE
                FROM 
                    AA_RESERVE_UTILIZATION
                WHERE 
                    PAIRING_DATE >= '2023-01-01'
                GROUP BY 
                    CASE 
                        WHEN PAIRING_POSITION = 'RO' THEN 'FO' 
                        WHEN PAIRING_POSITION IN ('FO', 'CA', 'FA') THEN PAIRING_POSITION 
                        ELSE 'FA' 
                    END, 
                    BASE, 
                    PAIRING_DATE, 
                    EQUIPMENT
            ) r
            FULL OUTER JOIN (
                -- Query for Sick table
                SELECT 
                    absence_date AS DATE, 
                    BASE, 
                    COALESCE(fleet, 'NA') AS EQUIPMENT,  
                    COALESCE(seat, 'FA') AS PAIRING_POSITION,  
                    COUNT(DISTINCT MASTID_EMPNO) AS NUM_SICK
                FROM AA_CONSOLIDATED_ABSENTEE
                WHERE CODE_LATEST_UPDATE IN ('1EF','1SC','1SK','2EF','2SK','3SK', '5EF','5SC','5SK','FLP','FLS','FLU','FLV','SIC','SK3','SOP')
                GROUP BY 
                    absence_date, 
                    base, 
                    fleet,
                    seat
            ) s
            ON r.DATE = s.DATE 
               AND r.BASE = s.BASE
               AND r.EQUIPMENT = s.EQUIPMENT
               AND r.PAIRING_POSITION = s.PAIRING_POSITION
            ORDER BY DATE, SEAT, FLEETTYPE, BASE;"
  
  sick_rsv_data <- dbGetQuery(db_connection_pg, OPL_q) %>% 
    filter(!FLEETTYPE == "33Y") %>% 
    mutate(FLEETTYPE = if_else(FLEETTYPE == "NA", NA, FLEETTYPE)) %>% 
    group_by(SEAT, DATE, FLEETTYPE, BASE) %>% 
    mutate(AVAILABLE_RESERVES = sum(AVAILABLE_RESERVES)) %>% 
    filter(SICK_LEAVE == max(SICK_LEAVE)) %>% 
    ungroup() %>% 
    distinct()
  
  open_time_data <- read_csv(here("data", "Open_Time.csv")) %>% 
    select(!c(Avail, Net)) %>% 
    mutate(FLEETTYPE = if_else(SEAT == "FA", NA, FLEETTYPE),
           DATE = ymd(mdy(DATE)),
           flag = if_else(FLEETTYPE == "33Y", 1, 0),
           flag = if_else(is.na(FLEETTYPE), 0, flag)) %>% 
    filter(flag == 0) %>% 
    select(!flag)
  
  transfer_data <- left_join(sick_rsv_data, open_time_data)
  
  write_csv(x=transfer_data, file="F:/INFLIGHT_RESERVE_OUTLOOK_ALL.csv")
  
  print("Data Uploaded")
  Sys.sleep(10)
  
}, error = function(cond) {
  print("An error occurred in the script:")
  print(cond)  # Print the actual error message
  Sys.sleep(10)  # Pause for 10 seconds to see the error message
})
