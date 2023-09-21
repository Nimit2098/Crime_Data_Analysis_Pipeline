# SFPD Crime Data Analysis| Modern Data Engineering GCP Project

## Introduction

The goal of this project is to create a data pipeline using Google Cloud Platform (GCP) services to process and analyze the San Francisco Crime dataset. Additionally, I aim to build an interactive dashboard using Looker Studio that provides insights into crime patterns, incident distribution, and police response in San Francisco.

## Architecture:
<img src="Architecture_PoliceCrime.jpeg">

## Explanation:
The San Francisco Crime dataset, available at this link: https://data.sfgov.org/Public-Safety/Police-Department-Incident-Reports-2018-to-Present/wg3w-h783 

Which contains a wide range of information related to reported incidents in the city. This dataset is a valuable resource for law enforcement agencies, policymakers, and researchers to understand and address crime-related challenges effectively.

To use the dataset from my github data folder, the user will have to run a command from the "Splitting_dataset.ipynb" file to join and recreate the original dataset which is in parts in the "Dataset" folder.

PS: The dataset named "Table_analytics.csv" is the analytics dataset joined in the bigquery after the data model creation.

## Data Model:
<img src="Data_model_PoliceCrime.jpeg">


## Data Pipeline using GCP:
1) Data Extraction: The first step in the data pipeline is to extract the dataset from the source. I used GCP's data ingestion tools or services like Cloud Storage to store the dataset securely.
2) Data Transformation: Data cleaning and transformation are crucial to prepare the dataset for analysis. I utilized GCP's data processing tools such as Google Compute Engine to clean, transform, and structure the data.
3) Data Storage: Processed data will be stored in a GCP data storage solution, such as BigQuery making it easily accessible for analysis.
4) Data Analysis: GCP offers a range of tools for data analysis, including BigQuery for SQL-based analysis and Cloud Dataprep for visual data exploration. I will leverage BigQuery tools to gain insights from the dataset.
5) Dashboard Creation: Looker Studio, a data exploration and visualization platform is used to design an interactive and user-friendly dashboard. Looker allows for creating customized dashboards that enable users to explore crime data visually.

## Data Pipeline(Mage-AI Interface)
<img src="Data_pipeline_PoliceCrime.jpg">

## Dashboard Features:

This is the link for the looker studio dashboard: https://lookerstudio.google.com/u/0/reporting/120a626e-d27e-479f-9bc6-ee105948ec47/page/p_14tcasr09c?s=sOCm1WkqR6w

The Looker Studio dashboard includes various features:

•	Crime Trends: Visualizations of crime trends over time, including daily, weekly, and monthly patterns.

•	Incident Distribution: Maps and charts showing the geographical distribution of reported incidents across neighborhoods and police districts.

•	Incident Categories: Charts depicting the distribution of incidents by category and subcategory.

•	Resolution Analysis: Insights into how incidents were resolved or closed.

•	Hotspot Identification: Identification of crime hotspots based on incident density and location.

## Dashboard User Interface
<img src="Dashboard_Interface.jpg">

## Advantages:

Creating a data pipeline and Looker Studio dashboard for the San Francisco Crime dataset offers several benefits:
1.	Crime Analysis: Law enforcement agencies can use the dashboard to identify crime patterns, allocate resources effectively, and devise strategies to reduce crime.
2.	Policy Insights: Policymakers and city officials can make data-driven decisions to improve public safety and allocate resources efficiently.
3.	Research Opportunities: Researchers and analysts can use the dataset and dashboard to conduct studies and gain insights into crime-related topics.
4.	Transparency: Providing access to crime data through an interactive dashboard enhances transparency and engages the community in understanding crime dynamics.

Overall, this project aims to leverage the power of GCP and Looker Studio to turn raw crime data into actionable insights, benefiting both the city of San Francisco and its residents.

Please let me know what you think about this project. Thank you for reading!

Happy Coding!!!