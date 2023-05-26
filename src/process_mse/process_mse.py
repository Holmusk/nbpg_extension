import neuroblu_postgres as nb
import jsonref
import warnings


def process_mse(cohort, version, window=None, columns=None, mapping=None, reference_col = 'index_date', dbname=None):
    """
    process mse data of patients
    This function returns the mse data of specified patients.
    Parameters
    ----------
    cohort (pd.DataFrame): 
        Minimum columns: person_id, index_date 
        The version of the schema as defined in OMOP CDM        
    version : CDM version
    window : window period to filter MSE data
        time window specified as a list [left cut-off , right cut-off] in days from the index date
        if None, the function will return data for all SAV measurements
    columns : `str, array-like of str`, *optional*
        SAVs of interest specified as str or array-like collection
        of str.
        If None, the function will return data for all SAV columns.
    mapping : `str, array-like of str`, *optional*
        SAV mappings of interest specified as a list of string/s.
        mapping has to be specified with two degrees of freedom separated with the semicolon delimiter
        in maincategory:subcategory format (i.e. Suicidal:suicidal_ideation)
        Please see list of SAV main and subcategories in Notion Page:
        https://www.notion.so/holmusk/SAV-3f7fe1348c044f599c3af27c4330582a
        If None, the function will return data for all SAV columns.                
    dbname: `string`, *optional*
        The name of the database as defined in ``db.json`` file.
        If None, the defaultDB is used.

    Returns
    A dataframe of SAV labels with measurement dates within the specified time window
    
    Example:
    time_window = [-14,365] # in days
    sav_mappings = ['Suicidal:"normal, no issues, not present"', 'Suicidal:"neutral, unable to categorize"', 'Suicidal:suicidal ideation']
    sav_columns = ["sav_221_suicidal_0","sav_222_suicidal_1","sav_223_suicidal_2"]                        
    
    df = process_mse(cohort[["person_id","index_date"]], version, window=time_period, mapping = sav_mappings)
    df = process_mse(cohort[["person_id","index_date"]], version, window=time_period, columns = sav_columns)

    """
    
    if mapping is not None:
        if len(mapping) == 0:
            raise ValueError("mapping contain no values")
        if columns is not None:
            raise ValueError("two types of mapping files given, please use only one")
        mapping_dictionary = jsonref.load(open('../config/sav_mapping_dictionary.json'))
        for mapping_name in mapping:
            if mapping_name not in list(mapping_dictionary.keys()):
                raise ValueError("mapping contains invalid SAV category:subcategory names")
        columns = list(map(mapping_dictionary.get, mapping) )

    df = nb.get_SAV(cohort['person_id'].tolist(), version, columns=columns, dbname=dbname)
    if df.empty:
        if len(df) == 0:
            raise ValueError("no person ids with sav labels")
    else:
        df = cohort.merge(df,on='person_id')
        if df['person_id'].nunique() != cohort['person_id'].nunique():
            warnings.warn("some person_ids has no sav labels")

    if window is not None:
        if len(window)!=2:
            raise TypeError("more than 2 args for time window")
        df = df[(df['measurement_date'] - df[reference_col]).dt.days >= window[0]]
        df = df[(df['measurement_date'] - df[reference_col]).dt.days <= window[1]]
    
    return df
