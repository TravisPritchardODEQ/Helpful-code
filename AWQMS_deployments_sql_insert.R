library(tidyverse)
library(openxlsx)
library(RODBC)
library(rgdal)
library(lubridate)
library(xlsx)




#file <- file.choose()
file <- "A:/AWQMS/VOLMON/Deployments/Combined_Deployments v2 - dates corrected.xlsx"
save_dir <- "//deqlead-lims/SERVERFOLDERS/AWQMS/Continuous/Continuous Data Scripts/test outputs/"
equipment_sheet = "Equipment from LASAR"
deployment_sheet = "Deployments from LASAR"



# Read deployments sheet from dataset
import_equipment <-openxlsx:: read.xlsx(file, sheet = equipment_sheet)
import_deployments <- openxlsx::read.xlsx(file, sheet = deployment_sheet)
import_deployments$startdate <-  as.POSIXct(import_deployments[["startdate"]] * (60*60*24)
                                             , origin="1899-12-30"
                                             , tz="GMT")
import_deployments$enddate <-  as.POSIXct(import_deployments[["enddate"]] * (60*60*24)
                                             , origin="1899-12-30"
                                             , tz="GMT")
import_deployments$startdate <- force_tz(import_deployments$startdate, tzone = "america/los_angeles")
import_deployments$enddate <- force_tz(import_deployments$enddate, tzone = "america/los_angeles")

import_deployments$startdate_date <- as.Date(import_deployments$startdate, tz ="america/los_angeles" )


# import_equipment[is.na(import_equipment)] <- ""
# import_deployments[is.na(import_deployments)] <- ""



# Connect to AWQMS and pull data for duplicate checks  -----------------------------


AWQMS.sql = odbcConnect('AWQMS')

AWQMS_Projects = sqlQuery(AWQMS.sql, "select project.org_uid, project.prj_id, project.prj_name,organization.org_id, organization.org_name
                          From dbo.project
                          Inner Join dbo.organization ON dbo.organization.org_uid=dbo.project.org_uid
                          ;", stringsAsFactors = FALSE) 

AWQMS_Equipment = sqlQuery(AWQMS.sql, "SELECT        organization.org_id, organization.org_name, equipment.eqp_id, equipment.eqp_name, equipment.eqp_comments
FROM            equipment INNER JOIN
                         organization ON equipment.org_uid = organization.org_uid", stringsAsFactors = FALSE)

AWQMS_deployments = sqlQuery(AWQMS.sql, "SELECT        organization.org_id, monitoring_location.mloc_id, equipment.eqp_id, equipment_deployment.eqpdpl_start_time, equipment_deployment.eqpdpl_end_time, CONVERT(date, equipment_deployment.eqpdpl_start_time) 
                         AS startdate, time_zone.tmzone_cd
FROM            organization INNER JOIN
                         monitoring_location ON organization.org_uid = monitoring_location.org_uid INNER JOIN
                         equipment_deployment ON organization.org_uid = equipment_deployment.org_uid AND monitoring_location.mloc_uid = equipment_deployment.mloc_uid INNER JOIN
                         time_zone ON equipment_deployment.tmzone_uid = time_zone.tmzone_uid INNER JOIN
                         equipment ON organization.org_uid = equipment.org_uid AND equipment_deployment.eqp_uid = equipment.eqp_uid")



odbcClose(AWQMS.sql)


# Projects ----------------------------------------------------------------

# Filter datafrane to be only distict org, project1, project2
# Build import string using mutate.
# proj Id is the leftmost 35 characters of proj name
unique_projects <- import_deployments %>%
  distinct(org, Project1, Project2, Project3) %>%
  gather(Project1, Project2, Project3, key = "type", value = "proj", na.rm = TRUE) %>%
  distinct(org, proj) %>%
  mutate(proj = trimws(proj, which = 'right')) %>%
  mutate(proj = gsub("'", "''", proj),
         chars = nchar(proj),
         prj_id = substr(proj, 1, 35)) %>%
  filter(!is.na(proj))


projects_not_AWQMS <- unique_projects %>%
  anti_join(AWQMS_Projects, by = c("org" = "org_id", "proj" = "prj_id")) 


projects_long <- projects_not_AWQMS %>%
  filter(chars > 35)

projects_sql <- projects_not_AWQMS %>%
  mutate(
    insert = paste0(
      "insert into project (org_uid, sdtyp_uid, usr_uid_create, usr_uid_last_change, prj_id, prj_name, prj_desc, prj_qapp_approved_yn, prj_qapp_approval_agency_name, prj_create_date, prj_last_change_date, prj_wqx_submit_required_yn, prj_wqx_submit_date, prj_beach_yn, evlog_uid_last_change, prj_comments, prj_private_yn, persnl_uid_manager) values ((select org_uid from organization where org_id = '",
      org,
      "'), NULL, NULL, NULL, '",
      prj_id,
      "','",
      proj,
      "', NULL, NULL, NULL, GETDATE(), GETDATE(), 'N', NULL, 'N', NULL, NULL, 'N', NULL)"
    )
  )


# Extract just the insert string
projects <- projects_sql$insert
# Add extra lines at beginning and at end of file
projects <- append(projects, "begin transaction;", after = 0)
projects <- append(projects, "rollback transaction;", after =length(projects))
projects <- append(projects, "commit transaction;;", after =length(projects))

#write txt files with sql extention
#write(projects, file = paste0(save_dir,"01 - projects v2.sql"))


# Equipment ---------------------------------------------------------------

# Filter equipment table to be only distict org,EquipmentType, ID, Name
# Build import string using mutate.


unique_equipment <- import_equipment %>%
  distinct(org, EquipmentType, ID, Name, .keep_all = TRUE) %>%
  anti_join(AWQMS_Equipment, by = c("org" = "org_id", "ID" = "eqp_id")) %>%
  mutate(Comments = gsub("'", "''", Comments))
  
  
  
Equipment_insert <- unique_equipment %>%  
  mutate(insert = paste0("insert into equipment (org_uid, sceqp_uid, eqp_id, eqp_comments, usr_uid_last_change, eqp_last_change_date, eqp_serial_num, eqp_model, eqp_name, eqp_qaqc, eqp_continuous_yn) values((select org_uid from organization where org_id = '",
                         org,"'), (select sceqp_uid from sample_collection_equip where sceqp_name = '",
                         EquipmentType,"'),'",
                         ID, "','",
                         Comments, "',1,getdate(),'','','",
                         Name, "','','Y');"))

#write(Equipment_insert$insert, file = "//deqlead-lims/SERVERFOLDERS/AWQMS/Continuous/Continuous Data Scripts/test outputs/02 - equipment.sql")



# Deployments -------------------------------------------------------------
AWQMS_deployments <- AWQMS_deployments %>%
  mutate(startdate = ymd(startdate)) 

unique_deployments <- import_deployments %>%
  distinct(org, EquipID,  Project1, Project2, station, startdate, enddate, Media, .keep_all = TRUE) %>%
  left_join(AWQMS_deployments, by = c("org" = "org_id", "EquipID" = "eqp_id", "Station" = "mloc_id","startdate_date" = "startdate" )) %>%
  filter(is.na(eqpdpl_start_time))

deployments_sql <-unique_deployments %>%
  mutate(insert = paste0("insert into equipment_deployment (eqp_uid, org_uid, mloc_uid, eqpdpl_frequency_minutes, eqpdpl_start_time, eqpdpl_end_time, eqpdpl_depth_meters, usr_uid_last_change, eqpdpl_last_change_date, acmed_uid, amsub_uid, tmzone_uid) values ((select eqp_uid from equipment where eqp_id = '",
                         EquipID,"' and org_uid = (select org_uid from organization where org_id = '",
                         org,"')),(select org_uid from organization where org_id = '",
                         org,"'),(select mloc_uid from monitoring_location where mloc_id = '",
                         Station,"' and org_uid = (select org_uid from organization where org_id = '",
                         org,"')),NULL,convert(datetime, replace('",
                         startdate,"','/','-'),121),convert(datetime, replace('",
                         enddate, "','/','-'),121),NULL,1,GETDATE(),(select acmed_uid from activity_media where acmed_name = '",
                         Media,"'),(select amsub_uid from activity_media_subdivision where amsub_name = ''),(select tmzone_uid from time_zone where tmzone_cd = '",
                         TimeZone,"'));"))


write(deployments_sql$insert, file = paste0(save_dir,"03 - deployments.sql"))  


# Deployments Projects ----------------------------------------------------

deployment_projects <- unique_deployments %>%
  gather(Project1, Project2, Project3, key = "type", value = "proj", na.rm = TRUE) %>%
  mutate(proj = trimws(proj, which = 'right')) %>%
  mutate(proj = gsub("'", "''", proj),
         chars = nchar(proj),
         prj_id = substr(proj, 1, 35)) %>%
  filter(!is.na(proj)) %>%
  mutate(insert = paste0("insert into equipment_deployment_project (eqpdpl_uid, prj_uid) values((select eqpdpl_uid from equipment_deployment where org_uid = (select org_uid from organization where org_id = '",
                         org,"') and eqp_uid = (select eqp_uid from equipment where eqp_id = '",
                         EquipID,"' and org_uid = (select org_uid from organization where org_id = '",
                         org,"')) and mloc_uid = (select mloc_uid from monitoring_location where mloc_id = '",
                         Station, "' and org_uid = (select org_uid from organization where org_id = '",
                         org, "')) and eqpdpl_start_time = convert(datetime, replace('",
                         startdate, "','/','-'),121)),(select prj_uid from project where org_uid = (select org_uid from organization where org_id = '",
                         org, "'and (prj_name = '",
                         proj, "' or prj_id = '",
                         prj_id, "')));"
                         ))
                         


write(deployment_projects$insert, file = paste0(save_dir,"05 - deployment_projects.sql"))     
    



    

