from neuroblu_postgres import utilities as util
from neuroblu_postgres.dbconfig import version_lookup
from psycopg2 import sql
import pandas as pd
import warnings

def get_days_available(cohort, version, dbname=None):
    """
    Gets observation period and number of days of data available before and after ``index_date``
        
    Parameters
    ----------
    cohort: pandas DataFrame containing ``person_id``, ``index_date``
    
    version: `string`
        The version of the schema as defined in OMOP CDM
    dbname: `string`, *optional*
        The name of the database as defined in ``db.json`` file.
        If None, the defaultDB is used.
    -------
    pandas.DataFrame
        Returns a data frame containing the ``person_id``, ``observation_period_start_date``, ``observation_period_end_date``,
        ``pre_index_days`` and ``post_index_days``
    
    Raises
    ------
    ValueError
        if ``cohort`` and expected columns are not passed in 
    UserWarning
        if the input is an empty list of patient IDs
    UserWarning
        if the ``index_date`` is out of the observation period recorded
        
    """
            
    # Check version
    util._is_valid_dbversion(version)
    schema_name = version_lookup[version]["cdt"]
    schema_name_vocab = version_lookup[version]["vocab"]
    cdm_version = version_lookup[version]["cdm"]

    # Validate the arguments
    if cohort is None:
        raise ValueError("Please enter 'cohort' arguments")
    if 'index_date' not in list(cohort.columns):
        raise ValueError("`index_date` must be a column in `cohort`")
        
    # Validate the patient ID
    patients_tuple = util._patient_id_to_tuple(list(cohort['person_id']))
    if len(patients_tuple) == 0:
        patients_tuple = (None,)
        warnings.warn("Trying to get data for 0 patients.")

    # Write query
    query_inter = """
            SELECT  op.person_id,
                    op.observation_period_start_date,
                    op.observation_period_end_date
            FROM    {schema_name}.observation_period AS op
            WHERE   op.person_id IN ({values});"""

    # fill & retrieve query
    query = sql.SQL(query_inter).format(
        schema_name=sql.Identifier(schema_name),
        values=sql.SQL(",").join(map(sql.Literal, patients_tuple)),
    )
    df = util.get_query(query, dbname=dbname)
    
    cohort_observation = cohort.merge(df)
    
    datetime_cols = ['index_date', 'observation_period_start_date', 'observation_period_end_date']
    cohort_observation[datetime_cols] = cohort_observation[datetime_cols].apply(pd.to_datetime, errors='coerce')

    cohort_observation['pre_index_days'] = (cohort_observation['index_date'] - cohort_observation['observation_period_start_date']).dt.days
    cohort_observation['post_index_days'] = (cohort_observation['observation_period_end_date'] - cohort_observation['index_date']).dt.days
    
    cohort_observation['index_before_start'] = cohort_observation['index_date'] < cohort_observation['observation_period_start_date']
    if True in list(cohort_observation['index_before_start'].unique()):
        warnings.warn(f"There are patients with index date occurring before the observation period start date")
    cohort_observation['index_after_end'] = cohort_observation['observation_period_end_date'] < cohort_observation['index_date']
    if True in list(cohort_observation['index_after_end'].unique()):
        warnings.warn(f"There are patients with index date occurring after the observation period end date")

    return cohort_observation[['person_id', 'index_date', 'observation_period_start_date', 'observation_period_end_date', 'pre_index_days', 'post_index_days']]
