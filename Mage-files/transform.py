import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here

    #Changing names of columns to insert Underscore in spaces.
    column_name = df.columns
    column_name_changed = list()
    for i in range(len(column_name)):
        column_name_changed.append(column_name[i].replace(" ","_"))
    df.columns = column_name_changed


    #standardizing the Report code respective to the report type description
    #Init of the dictionary
    Report_Code_correction = {
        'Vehicle Supplement' : 'VS',
        'Coplogic Initial' : 'CI',
        'Coplogic Supplement' : 'CS',
        'Initial' : 'II',
        'Initial Supplement' : 'IS',
        'Vehicle Initial': 'VI'
    }
    df['Report_Type_Code'] = df['Report_Type_Description'].map(Report_Code_correction)


    #to standardize the values in the Incident Category column that represent the same concept
    #Init of the dictionary
    replace_dict = {
        'Recovered Vehicle': 'Vehicle Theft and Recovery',
        'Motor Vehicle Theft': 'Vehicle Theft and Recovery',
        'Motor Vehicle Theft?': 'Vehicle Theft and Recovery',
        'Recovered Vehicle': 'Vehicle Theft and Recovery',
        'Larceny Theft': 'Property Crimes',
        'Lost Property': 'Property Crimes',
        'Stolen Property': 'Property Crimes',
        'Burglary': 'Property Crimes',
        'Vandalism': 'Property Crimes',
        'Drug Violation': 'Drug-related Incidents',
        'Drug Offense': 'Drug-related Incidents',
        'Non-Criminal': 'Miscellaneous',
        'Case Closure': 'Miscellaneous',
        'Other Miscellaneous': 'Miscellaneous',
        'Other Offenses': 'Miscellaneous',
        'Other': 'Miscellaneous',
        'Miscellaneous Investigation': 'Miscellaneous',
        'Suspicious Occ': 'Suspicious',
        'Suspicious': 'Suspicious',
        'Missing Person': 'Police Interaction Incidents',
        'Courtesy Report': 'Police Interaction Incidents',
        'Fraud': 'Fraudulent Activities',
        'Forgery And Counterfeiting': 'Fraudulent Activities',
        'Prostitution': 'Sex-related Incidents',
        'Human Trafficking, Commercial Sex Acts': 'Sex-related Incidents',
        'Human Trafficking (A), Commercial Sex Acts': 'Sex-related Incidents',
        'Human Trafficking (B), Involuntary Servitude': 'Sex-related Incidents',
        'Embezzlement': 'Financial and Regulatory Offenses',
        'Gambling': 'Financial and Regulatory Offenses',
        'Liquor Laws': 'Financial and Regulatory Offenses'
    }
    df['Incident_Category'].replace(replace_dict, inplace=True)


    #To fill the missing values in the Incident Category by using the respective Incident Description
    #Init of the dictionary
    description_to_incident = {
        'Vehicle, Seizure Order Service': 'Vehicle Theft and Recovery',
        'Gun Violence Restraining Order': 'Weapons Offense',
        'Driving, Stunt Vehicle/Street Racing': 'Traffic Collision',
        'Cryptocurrency Related Crime (secondary code only)': 'Financial and Regulatory Offenses',
        'Auto Impounded': 'Vehicle Impounded',
        'Theft, Boat': 'Property Crimes',
        'Theft, Animal, Att.': 'Property Crimes',
        'SFMTA Muni Transit Operator-Bus/LRV': 'SFMTA',
        'Military Ordinance': 'Weapons Offense',
        'SFMTA Parking and Control Officer': 'SFMTA',
        'Cloned Cellular Phone, Use': 'Fraudulent Activities',
        'Public Health Order Violation, Notification': 'Miscellaneous',
        'Public Health Order Violation, After Notification': 'Miscellaneous',
        'Assault, Commission of While Armed': 'Assault',
        'Theft, Phone Booth, <$50': 'Property Crimes',
        'Gun Violence Restraining Order Violation': 'Weapons Offense',
        'Theft, Phone Booth, $200-$950': 'Property Crimes',
        'Theft, Phone Booth, $50-$200': 'Property Crimes',
        'Vehicle, Seizure Order': 'Vehicle Theft and Recovery',
        'SFMTA Employee-Non Operator/Station Agent-Other Employee': 'SFMTA',
        'Assault, By Police Officers': 'Assault',
        'Crimes Involving Receipts or Titles': 'Fraudulent Activities',
        'Procurement, Pimping, & Pandering': 'Sex-related Incidents',
        'Service of Documents Related to a Civil Drug Abatement and/or Public Nuisance Action': 'Miscellaneous',
        'Pyrotechnic Explosive Device - Barrel Bomb': 'Fire Report',
        'Theft, Phone Booth, >$950': 'Property Crimes'
    }
    df['Incident_Category'] = df['Incident_Category'].fillna(df['Incident_Description'].map(description_to_incident))


    #converting columns to datetime format in df
    df['Incident_Datetime']= pd.to_datetime(df['Incident_Datetime'], format= '%Y/%m/%d %I:%M:%S %p')
    df['Report_Datetime']= pd.to_datetime(df['Report_Datetime'], format= '%Y/%m/%d %I:%M:%S %p')

    #Converting boolean column to binary column
    df['Filed_Online'] = df['Filed_Online'].notnull().astype(int)

    #Init of a dataframe for the dimension table
    df_time = pd.DataFrame(pd.date_range(start="2000-01-01", end="2000-01-02", freq="T", inclusive='left'))
    df_time['Id'] = df_time.index
    df_time['Time'] = df_time[0].dt.time
    df_time.drop(columns=[0], inplace=True)
    #To exrtact various tangents from the time columns.
    df_time['Hour'] = df_time['Time'].astype(str).str[:2].astype(int)

    #Init of a dataframe for the dimension table
    df_date = pd.DataFrame(pd.date_range(start=min(df['Incident_Datetime']), end=max(df['Incident_Datetime']), freq="D", inclusive='both'))
    df_date['Id'] = df_date.index
    df_date['Date'] = df_date[0].dt.date
    df_date.drop(columns=[0], inplace=True)
    df_date['Date'] = pd.to_datetime(df_date['Date'])
    #To exrtact various tangents from the date columns.
    df_date['Dayofmonth'] = df_date['Date'].dt.day
    df_date['Month_Number'] = df_date['Date'].dt.month
    df_date['Month_Name'] = df_date['Date'].dt.month_name()
    df_date['Dayofweek_number'] = df_date['Date'].dt.day_of_week
    df_date['Dayofweek_name'] = df_date['Date'].dt.day_name()
    df_date['Year'] = df_date['Date'].dt.year

    #Init of a dataframe for the dimension table
    df_report_info = df[['Report_Type_Code','Report_Type_Description']].copy()
    df_report_info.drop_duplicates(inplace=True)
    #df_report_info.drop(labels=[9], inplace=True)
    df_report_info.reset_index(inplace=True)
    df_report_info.drop(columns=['index'], inplace=True)

    #Init of a dataframe for the dimension table
    df_incident_info = df[['Incident_Code','Incident_Category','Incident_Subcategory','Incident_Description']].copy()
    df_incident_info.drop_duplicates(inplace=True)
    df_incident_info.drop(labels=[2029], inplace=True)
    df_incident_info.reset_index(inplace=True)
    df_incident_info.drop(columns=['index'], inplace=True)

    #Init of a dataframe for the dimension table
    df_police_district = df[['Police_District']].copy()
    df_police_district.drop_duplicates(inplace=True)
    df_police_district.reset_index(inplace=True)
    df_police_district.drop(columns=['index'], inplace=True)
    df_police_district['Police_district_Id'] = df_police_district.index
    df_police_district = df_police_district[['Police_district_Id', 'Police_District']]

        #Init of a dataframe for the dimension table
    df_resolution = df[['Resolution']].copy()
    df_resolution.drop_duplicates(inplace=True)
    df_resolution.reset_index(inplace=True)
    df_resolution.drop(columns=['index'], inplace=True)
    df_resolution['Resolution_Id'] = df_resolution.index
    df_resolution = df_resolution[['Resolution_Id', 'Resolution']]

    #Init of a dataframe for the dimension table
    df_neighborhood = df[['Analysis_Neighborhood']].copy()
    df_neighborhood.drop_duplicates(inplace=True)
    df_neighborhood.drop(labels=[0], inplace=True)
    df_neighborhood.reset_index(inplace=True)
    df_neighborhood.drop(columns=['index'], inplace=True)
    df_neighborhood['Neighborhood_Id'] = df_neighborhood.index
    df_neighborhood = df_neighborhood[['Neighborhood_Id', 'Analysis_Neighborhood']]
    df_neighborhood.rename(columns={'Analysis_Neighborhood': 'Neighborhood'}, inplace=True)

    #Init of a dataframe for the dimension table
    df_location = df[['Latitude','Longitude','Point']].copy()
    df_location.drop_duplicates(inplace=True)
    df_location.drop(labels=[0], inplace=True)
    df_location.reset_index(inplace=True)
    df_location.drop(columns=['index'], inplace=True)
    df_location['Location_Id'] = df_location.index
    df_location = df_location[['Location_Id', 'Latitude', 'Longitude', 'Point']]

    #Init of a dataframe for the dimension table
    df_intersection = df[['Intersection']].copy()
    df_intersection.drop_duplicates(inplace=True)
    df_intersection.drop(labels=[0], inplace=True)
    df_intersection.reset_index(inplace=True)
    df_intersection.drop(columns=['index'], inplace=True)
    df_intersection['Intersection_Id'] = df_intersection.index
    df_intersection = df_intersection[['Intersection_Id', 'Intersection']]

    #Init of a dataframe for the Fact database table
    df_fact_table = df.copy()

    #Converting boolean column to binary column
    #df_fact_table['Filed_Online'] = df_fact_table['Filed_Online'].notnull().astype(int)


    df_fact_table['Incident_Time_Id'] = df_fact_table['Incident_Datetime'].dt.time.map(df_time.set_index('Time')['Id'])
    df_fact_table['Incident_Date_Id'] = df_fact_table['Incident_Datetime'].dt.date.map(df_date.set_index('Date')['Id'])

    df_fact_table['Report_Time_Id'] = df_fact_table['Report_Datetime'].dt.time.map(df_time.set_index('Time')['Id'])
    df_fact_table['Report_Date_Id'] = df_fact_table['Report_Datetime'].dt.date.map(df_date.set_index('Date')['Id'])

    df_fact_table['Intersection_Id'] = df_fact_table['Intersection'].map(df_intersection.set_index('Intersection')['Intersection_Id'])
    df_fact_table['Police_district_Id'] = df_fact_table['Police_District'].map(df_police_district.set_index('Police_District')['Police_district_Id'])
    df_fact_table['Resolution_Id'] = df_fact_table['Resolution'].map(df_resolution.set_index('Resolution')['Resolution_Id'])
    df_fact_table['Neighborhood_Id'] = df_fact_table['Analysis_Neighborhood'].map(df_neighborhood.set_index('Neighborhood')['Neighborhood_Id'])

    df_fact_table = df_fact_table.merge(df_location, on=['Latitude', 'Longitude', 'Point'], how='left')


    #dropping the unwanted columns
    df_fact_table.drop(columns=['CAD_Number', 'CNN', 'Invest_In_Neighborhoods_(IIN)_Areas', 'ESNCAG_-_Boundary_File', 'Central_Market/Tenderloin_Boundary_Polygon_-_Updated', 'Civic_Center_Harm_Reduction_Project_Boundary', 
                    'HSOC_Zones_as_of_2018-06-05', 'Supervisor_District', 'Supervisor_District_2012', 'Latitude', 'Latitude', 'Point', 'Row_ID', 'Incident_Datetime', 'Incident_Date', 'Incident_Time', 'Incident_Year',
                    'Incident_Day_of_Week', 'Report_Datetime', 'Report_Type_Description', 'Incident_Category', 'Incident_Subcategory', 'Incident_Description', 'Resolution','Intersection', 'Police_District', 'Analysis_Neighborhood',
                    'Longitude', 'Neighborhoods', 'Current_Supervisor_Districts', 'Current_Police_Districts'], inplace=True)


    df_fact_table.dropna(inplace=True)

    #adding columns for the primary key
    df_fact_table.reset_index(inplace=True)
    df_fact_table.drop(columns=['index'], inplace=True)
    df_fact_table['Id'] = df_fact_table.index


    #to order the columns in fact database
    df_fact_table = df_fact_table[['Id', 'Incident_ID', 'Incident_Number', 'Incident_Date_Id', 'Incident_Time_Id', 'Report_Date_Id', 'Report_Time_Id', 'Incident_Code', 'Report_Type_Code', 'Filed_Online', 'Police_district_Id', 'Resolution_Id',
                                'Location_Id', 'Neighborhood_Id', 'Intersection_Id']]

    #converting the column id's to integer type
    df_fact_table[['Location_Id', 'Neighborhood_Id', 'Intersection_Id']] = df_fact_table[['Location_Id', 'Neighborhood_Id', 'Intersection_Id']].astype(int)


    return {"date_dim":df_date.to_dict(orient="dict"),
    "time_dim":df_time.to_dict(orient="dict"),
    "report_info_dim":df_report_info.to_dict(orient="dict"),
    "location_dim":df_location.to_dict(orient="dict"),
    "intersection_dim":df_intersection.to_dict(orient="dict"),
    "neighborhood_dim":df_neighborhood.to_dict(orient="dict"),
    "resolution_dim":df_resolution.to_dict(orient="dict"),
    "police_district_dim":df_police_district.to_dict(orient="dict"),
    "incident_info_dim":df_incident_info.to_dict(orient="dict"),
    "crime_incidents_fact_table":df_fact_table.to_dict(orient="dict")}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
