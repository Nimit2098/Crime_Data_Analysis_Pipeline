CREATE OR REPLACE TABLE crime-analysis-399301.crime_us_dataengineering.tbl_analystics AS (
SELECT f.Id,
       f.Incident_Id,
       f.Incident_Number, 
       dti.Date AS Incident_Date,
       dti.Dayofmonth AS Incident_Dayofmonth,
       dti.Month_Number AS Incident_Month_Number,
       dti.Month_Name AS Incident_Month_Name,
       dti.Dayofweek_number AS Incident_Dayofweek_number, 
       dti.Dayofweek_name AS Incident_Dayofweek_name, 
       dti.Year AS Incident_Year,
       ti.Time AS Incident_Time,
       ti.Hour AS Incident_Hour, 
       dtr.Date AS Report_Date,
       dtr.Dayofmonth AS Report_Dayofmonth,
       dtr.Month_Number AS Report_Month_Number,
       dtr.Month_Name AS Report_Month_Name,
       dtr.Dayofweek_number AS Report_Dayofweek_number, 
       dtr.Dayofweek_name AS Report_Dayofweek_name, 
       dtr.Year AS Report_Year,
       tr.Time AS Report_time, tr.Time AS Report_Hour,
       inc.Incident_Code,
       inc.Incident_Category,
       inc.Incident_Subcategory,
       inc.Incident_Description,
       f.Report_Type_Code,
       rec.Report_Type_Description,
       f.Filed_Online,
       pd.Police_District,
       res.Resolution,
       loc.Latitude,loc.Longitude,loc.Point,
       nbh.Neighborhood,
       itr.Intersection
FROM crime-analysis-399301.crime_us_dataengineering.crime_incidents_fact_table AS f
JOIN crime-analysis-399301.crime_us_dataengineering.date_dim AS dti
ON f.Incident_Date_Id = dti.Id
JOIN crime-analysis-399301.crime_us_dataengineering.time_dim AS ti
On f.Incident_Time_Id = ti.Id
JOIN crime-analysis-399301.crime_us_dataengineering.date_dim AS dtr
ON f.Report_Date_Id = dtr.Id
JOIN crime-analysis-399301.crime_us_dataengineering.time_dim AS tr
On f.Report_Time_Id = tr.Id
JOIN crime-analysis-399301.crime_us_dataengineering.incident_info_dim AS inc
ON f.Incident_Code = inc.Incident_Code
JOIN crime-analysis-399301.crime_us_dataengineering.report_info_dim AS rec
ON f.Report_Type_Code = rec.Report_Type_Code
JOIN crime-analysis-399301.crime_us_dataengineering.police_district_dim AS pd
ON f.Police_district_Id = pd.Police_district_Id
JOIN crime-analysis-399301.crime_us_dataengineering.resolution_dim AS res
On f.Resolution_Id = res.Resolution_Id
JOIN crime-analysis-399301.crime_us_dataengineering.location_dim AS loc
ON f.Location_Id = loc.Location_Id
JOIN crime-analysis-399301.crime_us_dataengineering.neighborhood_dim AS nbh
ON f.Neighborhood_Id = nbh.Neighborhood_Id
JOIN crime-analysis-399301.crime_us_dataengineering.intersection_dim AS itr
ON f.Intersection_Id = itr.Intersection_Id
ORDER BY f.Id ASC)
;