from pandas.api.types import is_datetime64_any_dtype
from datetime import timedelta
import neuroblu_postgres as nb
import warnings

def calculate_HCRU(patients, period_start, period_end, version, visit_mapping=None, reference_col='index_date', failed_encounter=True):
    """
    This function calculates the healthcare resource utilization (HCRU) of specified patients present in a specified period.
    The period is defined is [reference col + period start, reference col + period end].
    
    Notes
    -----
    HCRU may additionally be defined in cost terms. However, this function only calculates the utilization in visits.
    
    Parameters
    ----------
    patients: pandas DataFrame containing ``person_id`` and the reference column specified (by default, ``index_date``)
        Cohort to calculate HCRU
    period_start:  `integer`
        Start of the period for HCRU calculation, with reference to the reference column
    period_end: `integer`
        End of the period for HCRU calculation, with reference to the reference column
    version: `string`
        The version of the schema as defined in OMOP CDM
    visit_mapping: `dict`
        Mapping of HCRU variables of interest to specific visit types as key-value pairs. By default 1-1 mapping is used for OP, IP, ED.
    reference_col: `str`
        Reference column to define the period for HCRU calculation
    failed_encounter: `bool`
        If False, filters out visits that are 'failed encounters'. By default True.
    Returns
    -------
    pandas.DataFrame
        Returns a data frame containing the ``person_id`` and number of visits in the period specified
        The new columns are labled by {key}_{period_start}-{period_end} for each key in visit_mapping
    
    Raises
    ------
    TypeError
        if ``period_start`` or ``period_end`` is not type int
    TypeError
        if ``visit_mapping`` is not type dict
    TypeError
        if ``reference_col`` is not type datetime in the patients DataFrame
    ValueError
        if ``period_start`` is larger than ``period_end`` 
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
        
    if not isinstance(period_start, int):
        raise TypeError(f"`period_start` should be an integer")
    if not isinstance(period_end, int):
        raise TypeError(f"`period_end` should be an integer")
    if not isinstance(visit_mapping, dict):
        raise TypeError(f"`visit_mapping` should be a dictionary")
    if not is_datetime64_any_dtype(patients[reference_col]):
        raise TypeError(f"`reference_col` should be a datetime object in patients")
    if period_start > period_end:
        raise ValueError("`period_start` should not be larger than `period_end`")
    period_duration = (period_end - period_start)+1
    
    # Calculate HCRU
    for i in visit_mapping.keys():
        # Version check, patient id, visit type, bool check is done in nb.get_visit
        df_visit = nb.get_visit(patients=tuple(patients.person_id), visit_types=visit_mapping[i], version=version, failed_encounter=failed_encounter)
        patients_visits = patients.merge(df_visit, on='person_id')

        patients_visits = patients_visits[(patients_visits.visit_start_date <= (patients_visits[reference_col] + timedelta(days=period_end))) &
                                     (patients_visits.visit_end_date >= (patients_visits[reference_col] + timedelta(days=period_start)))]
    
        # Drop duplicates where there are more than 1 visit type specified and may over count across visit types
        patients_visits = patients_visits.sort_values(['person_id', 'visit_start_date', 'visit_end_date'])
        # Case 1: 1 Jan - 5 Jan IP, 1 Jan - 1 Jan ED
        patients_visits = patients_visits.drop_duplicates(subset=['person_id', 'visit_start_date'])
        # Case 2: 1 Jan - 5 Jan IP, 5 Jan - 5 Jan ED
        patients_visits = patients_visits.drop_duplicates(subset=['person_id', 'visit_end_date'])
        # Case 2: 1 Jan - 5 Jan IP, 3 Jan - 3 Jan ED
        patients_visits['next_visit_start'] = patients_visits['visit_start_date'].shift(-1)
        patients_visits['next_person_id'] = patients_visits['person_id'].shift(-1)
        patients_visits = patients_visits[~((patients_visits.next_person_id==patients_visits.person_id) & 
                                            (patients_visits.next_visit_start >= patients_visits.visit_start_date) & 
                                            (patients_visits.next_visit_start <= patients_visits.visit_end_date))]
        
        number_of_visits_in_period = patients_visits.groupby('person_id').agg({'visit_start_date':'nunique'}).reset_index()
    
        number_of_visits_in_period = number_of_visits_in_period.rename(columns={'visit_start_date': f'{i}_{period_start}-{period_end}'})

        if number_of_visits_in_period['visit_start_date'].any() > period_duration:
            warnings.warn(f"Number of {i} visit calculated for the period of days {period_start} to {period_end} exceeds the number of days in the period")
        
        patients = patients.merge(number_of_visits_in_period, on='person_id', how='left')
        patients = patients.fillna(0)
        
    return patients
