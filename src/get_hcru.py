from pandas.api.types import is_datetime64_any_dtype
from datetime import timedelta
from neuroblu_postgres.preprocessing import merge_prescriptions
import warnings

def calculate_HCRU(cohort, version, visit_mapping=None, failed_encounter=True):
    """
    This function calculates the healthcare resource utilization (HCRU) of the cohort passed in in a specified period.
    
    Notes
    -----
    HCRU may additionally be defined in cost terms. However, this function only calculates the utilization in visits.
    
    Parameters
    ----------
    cohort: pandas DataFrame containing ``person_id``, ``start_date``, ``end_date``, ``cohort_id``
        Cohort with time period to calculate HCRU 
        HCRU is calculated in the time period of [start_date, end_date]
    version: `string`
        The version of the schema as defined in OMOP CDM
    visit_mapping: `dict`
        Mapping of HCRU variables of interest to define visit types as key-value pairs. 
        By default 1-1 mapping is used for OP, IP, ED.
        To get the full list of values available, please use nb.get_unique_values('visit type').
    failed_encounter: `bool`
        If False, filters out visits that are 'failed encounters'. By default True.
    Returns
    -------
    pandas.DataFrame
        Returns a data frame containing the ``person_id`` and number of each visit type in the period specified
        The new columns are labelled by {key} for each key in visit_mapping.
    
    Raises
    ------
    TypeError
        if ``visit_mapping`` is not type dict
    UserWarning
        if number of HCRU visits exceeds number of days in period specified
        
    """
    # Validate the inputs
    if visit_mapping == None:
        visit_mapping = {
        "OP": ["Outpatient Visit"],
        "IP": ["Inpatient Visit"],
        "ED": ["Emergency Room Visit"]
        }
            
    if not isinstance(visit_mapping, dict):
        raise TypeError(f"`visit_mapping` should be a dictionary")
    period_duration = (cohort['end_date'] - cohort['start_date']).dt.days+1
    
    cohort_input_info = cohort.copy()
    
    # Calculate HCRU
    for i in visit_mapping.keys():
        # Version check, patient id, visit type, bool check is done in nb.get_visit
        df_visit = nb.get_visit(cohort=cohort_input_info, visit_types=visit_mapping[i], version=version, failed_encounter=failed_encounter)
        
        # Handle overlapping visits which may occur in IP records / 2 visits classified as same visit type in visit mapping
        patients_visits = merge_prescriptions(df_visit, gap=0, drug_start_date='visit_start_date', drug_end_date='visit_end_date')        
        number_of_visits_in_period = patients_visits.groupby('person_id').agg({'visit_start_date':'nunique'}).reset_index()
        
        cohort = cohort.merge(number_of_visits_in_period, on='person_id', how='left')
        cohort = cohort.fillna(0)
        cohort = cohort.rename(columns={'visit_start_date': f'{i}'})
        cohort[i] = cohort[i].astype(int)

    cohort['period_duration'] = period_duration
    for i in visit_mapping.keys():
        cohort['check_validity'] = (cohort[i] > cohort['period_duration'])
        if True in list(cohort['check_validity'].unique()):
            warnings.warn(f"Number of {i} visit calculated for the period exceeds the number of days in the period")
        
    return cohort
