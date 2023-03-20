import neuroblu_postgres as nb
import pandas as pd
import warnings

def get_aggregated_measurement(scale, version, patients=None, cohort=None, failed_encounter=True, dbname=None,
                            agg='mode_median'):
    
    """
    This function returns a filtered summary/total score for a given measurement for a set of patients based on criteria set.
    20/03/2023: this function only supports CGI-S, CGI-I and GAF
    
    Parameters
    ----------
    version: `string`
        The version of the schema as defined in OMOP CDM
    patients : `integer, array-like of integers`
        Patient IDs of interest specified as integer or array-like
        collection of integers.
    cohort : pandas DataFrame containing ``person_id``, ``start_date``, ``end_date``, and ``cohort_id``
    dbname: `string`, *optional*
        The name of the database as defined in ``db.json`` file.
        If None, the defaultDB is used.
    failed_encounter: `bool`
        If False, filters out visits that are 'failed encounters'.
        By default True.
    agg: 'string'
        How the scales should be aggregated. Options available are ``mean``, ``median``,``max``,``min``,``mode_median``,``mode_mean``,``mode_max``,``mode_min``
        ``mode_median``: aggregate using mode first, then median if multiple modes
        
    Returns
    -------
    pandas.DataFrame
        Returns a data frame containing the ``person_id`` ,``agg_value``
    
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
    
    
    # aggregate the scales (!!! TO-DO - code can be cleaner & more straightforward !!!)
    if agg == 'mean':
        agg_df = round(df.groupby('person_id')['value'].mean().rename('agg_value',axis=0),0).astype(int).to_frame().reset_index()

    if agg == 'median':
        agg_df = round(df.groupby('person_id')['value'].median().rename('agg_value',axis=0),0).astype(int).to_frame().reset_index()

    if agg == 'max':
        agg_df = round(df.groupby('person_id')['value'].max().rename('agg_value',axis=0),0).astype(int).to_frame().reset_index()
        
    if agg == 'min':
        agg_df = round(df.groupby('person_id')['value'].min().rename('agg_value',axis=0),0).astype(int).to_frame().reset_index()
    
    #aggregate using mode first, then median if multiple modes
    if agg == 'mode_median':
        agg_df = round(df.groupby('person_id')['value'].agg(pd.Series.mode).groupby('person_id').median().rename('agg_value',axis=0),0).astype(int).to_frame().reset_index()
    
    #aggregate using mode first, then mean if multiple modes
    if agg == 'mode_median':
        agg_df = round(df.groupby('person_id')['value'].agg(pd.Series.mode).groupby('person_id').mean().rename('agg_value',axis=0),0).astype(int).to_frame().reset_index()
    
    #aggregate using mode first, then min if multiple modes
    if agg == 'mode_min':
        agg_df = round(df.groupby('person_id')['value'].agg(pd.Series.mode).groupby('person_id').min().rename('agg_value',axis=0),0).astype(int).to_frame().reset_index()
    
    #aggregate using mode first, then max if multiple modes
    if agg == 'mode_max':
        agg_df = round(df.groupby('person_id')['value'].agg(pd.Series.mode).groupby('person_id').max().rename('agg_value',axis=0),0).astype(int).to_frame().reset_index()
    
    assert len(agg_df) == df.person_id.nunique(), "Aggregation error. There shoule be an aggregated value per unique person id."
    
    # (!!! TO-DO - obtain the first/last measurement date of the aggregated value !!!)
    # (!!! TO-DO - further extension: obtain patients with increase/decrease in scale by X within Y period !!!)
    
    return agg_df