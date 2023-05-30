import neuroblu_postgres as nb
import numpy as np
from datetime import timedelta
import pandas as pd
import warnings

def get_aggregated_measurement(scale, version, patients=None, cohort=None, failed_encounter=True, dbname=None,
                            agg='mode_median',change=False):
    
    """
    This function returns an aggregated summary/total score for a given measurement for a set of patients based on parameters set.
    20/03/2023: this function only supports CGI-S, CGI-I and GAF
    
    Parameters
    ----------
    scale : `str`
        The name of the measurement scale being retrieved.
    version : `string`
        The version of the schema as defined in OMOP CDM
    patients : `integer, array-like of integers`
        Patient IDs of interest specified as integer or array-like
        collection of integers.
    cohort : pandas DataFrame containing ``person_id``, ``start_date``, ``end_date``, and ``cohort_id``
    dbname : `string`, *optional*
        The name of the database as defined in ``db.json`` file.
        If None, the defaultDB is used.
    failed_encounter : `bool`
        If False, filters out visits that are 'failed encounters'.
        By default True.
    agg : 'string'
        How the scales should be aggregated. Options available are ``mean``, ``median``,``max``,``min``,``mode_median``,``mode_mean``,``mode_max``,``mode_min``
        ``mode_median``: aggregate using mode first, then median if multiple modes
    change : 'boolean'
        If True, scale measurements will be aggregated per person_id per measurement day using the selected agg method.
        Outputs the change in measurements from the previous available measurement and the number of days between the change.
        
    Returns
    -------
    pandas.DataFrame
        Returns a data frame containing the ``person_id`` ,``agg_value``
        If change = True, returns *patients with at least 2 measurements* and a data frame containing the 
        ``person_id`` ,``prev_measurement_date``,``prev_value``,
        ``measurement_date``,``value``, 
        ``days_between_change``,``value_change`` 
    
    Raises
    ------
    TypeError
        if ``patients`` is not one of the following -
        int, list of int, tuple, numpy array, pandas series of int.
        Also if `person_id` in ``cohort`` is not type int.
    ValueError
        if both ``patients`` and ``cohort`` are None or not None together.
    TypeError
        if ``cohort`` is supplied and is not a dataframe.
    TypeError
        if `start_date` and `end_date` in ``cohort`` are not datetime.
    ValueError
        if ``cohort`` is supplied and does not have the required columns
    TypeError
        if `failed_encounter` is not type bool
    UserWarning
        if the input is an empty list of patient IDs
    UserWarning
        if the scale input does not exist in this dataset.
    UserWarning
        if the CGIS or CGII value contains any 0s
        
    """
    # validate aggregation
    if agg not in ['mean','median','mode','max','min','mode_median','mode_mean','mode_min','mode_max']:
        warnings.warn("Aggregation method not found.")
        return None
    
    # call CGIS, CGII or GAF
    
    # error: can't call a nb internal function ):
    # df = nb._get_measurement_summary(scale=scale, patients=patients, cohort=cohort, failed_encounter=failed_encounter, dbname=dbname, version=version)

    if scale == "CGI-S":
        df = nb.get_CGIS(version=version, patients=patients, cohort=cohort, failed_encounter=failed_encounter, dbname=dbname)
    if scale == "CGI-I":
        df = nb.get_CGII(version=version, patients=patients, cohort=cohort, failed_encounter=failed_encounter, dbname=dbname)
    if scale == "GAF":
        df = nb.get_GAF(version=version, patients=patients, cohort=cohort, failed_encounter=failed_encounter, dbname=dbname)
    
    # groupby person_id, groupby person_id then measurement date if looking for change in scales.
    if change is True:
        groupby = ['person_id','measurement_date']
    else:
        groupby = 'person_id'
    df_groupby = df.groupby(groupby)['value']
    
    # aggregate the scales
    if agg == 'mean':
        agg_df = df_groupby.mean()
    if agg == 'median':
        agg_df = df_groupby.median()
    if agg == 'max':
        agg_df = df_groupby.max()
    if agg == 'min':
        agg_df = df_groupby.min()
    
    # aggregate using mode first,
    df_groupby_mode = df_groupby.apply(pd.Series.mode).groupby(groupby)

    # then median, mean, min or max if multiple modes
    if agg == 'mode_median': 
        agg_df = df_groupby_mode.median()
    if agg == 'mode_mean': 
         agg_df = df_groupby_mode.mean()
    if agg == 'mode_min': 
        agg_df = df_groupby_mode.min()
    if agg == 'mode_max': 
        agg_df = df_groupby_mode.max()
    
    clean_agg_df = round(agg_df,0).astype(int).to_frame().reset_index()
    
    if change is False:
        assert len(clean_agg_df) == df.person_id.nunique(), "Aggregation error. There shoule be an aggregated value per unique person id."
    else:
        # obtain patients with increase/decrease in scale
        clean_agg_df["prev_person_id"] = clean_agg_df['person_id'].shift(1)
        clean_agg_df["prev_measurement_date"] = clean_agg_df['measurement_date'].shift(1)
        clean_agg_df["prev_value"] = clean_agg_df['value'].shift(1)


        clean_agg_df["measurement_date"] = pd.to_datetime(clean_agg_df["measurement_date"])
        clean_agg_df["prev_measurement_date"] = pd.to_datetime(clean_agg_df["prev_measurement_date"])
        
        # days between the change
        clean_agg_df["days_between_change"] = np.where(clean_agg_df["person_id"] == clean_agg_df["prev_person_id"], (clean_agg_df['measurement_date']-clean_agg_df["prev_measurement_date"]).dt.days,pd.Timedelta("nan"))
        # change in value
        clean_agg_df["value_change"] = np.where(clean_agg_df["person_id"] == clean_agg_df["prev_person_id"], (clean_agg_df['value']-clean_agg_df["prev_value"]),np.nan)
        #filter for patients with at least 2 measurements
        clean_agg_df = clean_agg_df[["person_id","prev_measurement_date","prev_value","measurement_date","value","days_between_change","value_change"]].dropna(subset=['value_change'])
    
    return clean_agg_df