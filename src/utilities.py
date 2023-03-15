from psycopg2 import sql
import pandas as pd
import numpy as np
import numbers
import warnings
from .dbconfig import DBVersionError, version_lookup
from .databaseIO import pgIO


def _is_patient_id(patients):
    """
    A function to check if the input ``patients`` is a valid patient ID
    Parameters
    ----------
    patients : `list, tuple, pandas.Series, numpy.ndarray (0d or 1d)`
        Sequence of patient IDs to be checked for validity
    Returns
    -------
    tuple :
        Sequence of patient IDs as tuple data type
    Raises
    ------
    TypeError
        If the input is not a integer, or a list, array, or tuple of integers
    ValueError
        If the input contains negative values
    """
    if isinstance(patients, np.ndarray) or isinstance(patients, tuple) or isinstance(patients, list) or isinstance(patients, numbers.Integral):
        if isinstance(patients, np.ndarray):
            patients = patients.flatten().tolist()
        elif isinstance(patients, numbers.Integral):
            patients = [patients]
        if not all(isinstance(item, numbers.Integral) for item in patients):
            raise TypeError("'patients' contains invalid patient IDs")
        if not all(item > 0 for item in patients):
            raise ValueError("'patients' contains invalid patient IDs")
    else:
        raise TypeError("'patients' should be a non-negative integer, or a list, NumPy array, or tuple of non-negative integers")
    return None


def _patient_id_to_tuple(patients):
    """
    Converts a single integer, or iterable of integers, into a tuple.
    Parameters
    ----------
    patients : `int, list, tuple, pandas.Series or numpy.ndarray (0d or 1d)`
        Patient ID(s) to be converted into a tuple.
    Returns
    -------
    tuple :
        A tuple of patient IDs if the ``patients`` does not contain any
        invalid patient IDs
    Raises
    ------
    TypeError
        If the input array contains non-integers
    ValueError
        If the input array contains negative values
    """
    # perform error checking first
    _is_patient_id(patients)

    # convert patients to valid tuple
    if isinstance(patients, numbers.Integral):
        patients = (int(patients),)

    elif isinstance(patients, list) or isinstance(patients, tuple) or isinstance(patients, np.ndarray):
        if isinstance(patients, np.ndarray):
            patients = patients.flatten().tolist()
        patients = tuple(patients)

    return patients


def get_query(query, parameters=None, dbname=None):
    """
    Get Results of a query run on NeuroBlu
    This function gets the result of a query run on NeuroBlu, then
    returns the result as a pandas dataframe.
    Parameters
    ----------
    query : `string`
        Contains the SQL statement to be queried
    parameters : `tuple`, *optional*
        Contains the parameters to be passed on to the query for substituting
        the placeholders
    Returns
    -------
    pandas.DataFrame
        Output of the SQL query as a pandas DataFrame object
    """
    if parameters is not None:
        if not isinstance(parameters, (list, tuple)):
            raise TypeError("The parameters must be of type list or tuple")

    output, col_names = pgIO.getAllData(query, parameters, dbName=dbname, return_colnames=True)
    df = pd.DataFrame(output, columns=col_names)
    return df


def _is_valid_filter(values, dtype=str):
    """
    A function to check if the input `values` is a valid filter,
    or are valid filters.
    Parameters
    ----------
    values : `list or tuple`
        Filter values to be checked for validity
    type: `A recognized python built-in type`
        Type to check values against
    Raises
    ------
    TypeError
        If the input array contains invalid values.
    """

    if not (isinstance(values, list) or isinstance(values, tuple) or isinstance(values, dtype)):
        raise TypeError(f"'values' should be type {dtype.__name__}, a list, or a tuple")

    if not isinstance(values, dtype):
        if not all(isinstance(item, dtype) for item in values):
            raise TypeError(f"'values' contains non-{dtype.__name__} values")


def _values_to_tuple(values, dtype=str, lower=True):
    """
    Converts a single element, or a list or tuple of elements, into a tuple.
    Parameters
    ----------
    values : `str/int, or a list or tuple of such`
        Filter values to be converted into a tuple
    type: `A recognized python built-in type`
        Type to check values against. By default `str`
    lower: `boolean`
        If True, lowercases string inputs. By default True
    Returns
    -------
    tuple :
        Filter values output as a tuple. If values are strings and lower==True, they are returned lowercased.
    Raises
    ------
    TypeError
        If the input is not a str/int, or a list or tuple of such.
    """
    # perform error checking first
    _is_valid_filter(values, dtype)

    # convert valid output to tuple
    if isinstance(values, dtype):
        values = (values,)

    elif isinstance(values, list) or isinstance(values, tuple):
        values = tuple(values)

    # lowercase elements if type is string
    if dtype is str and lower:
        values = tuple([value.lower() for value in values])
    return values


def _is_valid_dbversion(version):
    """
    Check if dbversion is valid
    Parameters
    ----------
    version : `str`
        Version of the database to check.
    Raises
    ------
    DBVersionError
        This is a custom error type, defined in dbconfig.py.
        Raises if the input does not exist in valid_dbversion.
    """
    db_version_error = DBVersionError(version)
    if version not in db_version_error.valid_dbversion:
        raise db_version_error


def _make_nice_messages(your_list, last_sep=" or ", sep=", ", put_quotes=True):
    """
    Turn your list into a nice string for messages for errors/warnings
    Parameters
    ----------
    your_list : list
        a list to turn into a str
    last_sep : str, optional
        The final separator for ur elements in list, by default " or "
    sep : str, optional
        common separator for your elements in list, by default ", "
    Returns
    -------
    string
        a long string of concatenated items in ur list
    """
    # put in quotes
    if put_quotes:
        j = [f"'{i}'" for i in your_list]
    else:
        j = [str(i) for i in your_list]

    # add space accordingly
    if list(last_sep)[0] != " ":
        last_sep = " " + last_sep

    d = {}  # add variables into dictionary keys
    for i in (sep, last_sep):
        if list(i)[-1] != " ":
            d["{}".format(i)] = i + " "
        else:
            d["{}".format(i)] = i

    # get your list
    list_to_return = d[str(last_sep)].join([d[str(sep)].join(j[:-1]), j[-1]] if len(j) > 2 else j)
    return list_to_return


def _is_valid_version_for_measurement(version):
    invalid_version = ["r20r1", "r21r1", "rel_21r2"]
    valid_version = [i for i in version_lookup.keys() if i not in invalid_version]
    if version in invalid_version:
        valid = _make_nice_messages(valid_version)
        invalid = _make_nice_messages(invalid_version, "and")
        warnings.warn(
            f"""Please use: {valid} instead as PHQ9, BNSA, BDSS, CSSRS, DLA20, PSRS, QIDS, AUDITC & FTND are not available for: {invalid}"""
        )


def write_db(df, schemaname, tablename, change_user="ds_role", drop_ifexists=False, dbname=None):
    """Write dataframe objects to Postgres database.
    Parameters
    ----------
    df : `pandas.DataFrame`
        Pandas dataframe to be written to the database
    schemaname : `str`
        Name of the schema
    tablename : `str`
        Name of the table
    change_user: `str` or None
        Name of the role to transfer ownership. Specify None if no transferring is intended, by default 'ds_role'
    drop_ifexists : `bool`, *optional*
        Indicates if the table needs to be dropped if exists, by default False
    dbname : `str`, *optional*
        Name of the Postgres database, by default None
    Returns
    -------
    `True or None`
        A successful completion of this function returns a ``True``.
        In case there is an error, an exception would be raised.
    """
    # Check if change_user is a string object
    if not isinstance(change_user, str):
        warnings.warn("`change_user` argument is not string. The ownership of the table cannot be transferred!")
        change_user = None

    # Check if the column names begin with an integer
    if type(df.columns) is pd.Index:
        if any(c[0].isdigit() for c in df):
            warnings.warn(
                "The dataframe contains columns with names that begin with a digit. Please rename these columns to create the tables on the database."
            )
            return

    # The following list consists of tuples when it is a multi index hence the slice on [0]
    elif type(df.columns) is pd.MultiIndex:
        if any(c[0][0].isdigit() for c in df):
            warnings.warn(
                "The dataframe contains columns with names that begin with a digit. Please rename these columns to create the tables on the database."
            )
            return

    # Check if the table names begin with an integer
    if tablename[0].isdigit():
        warnings.warn(
            "The `tablename` begins with a digit. Please use another name to create the tables on the database."
        )
        return

    # Warning if columnnames/tablenames has white spaces
    if type(df.columns) is pd.Index:
        if any(" " in c for c in df):
            warnings.warn(
                "The dataframe contains columns with names that has white spaces. These would be converted to underscores while writing to the database."
            )
        df.columns = [c.replace(" ", "_") for c in df]

    # The following list consists of tuples when it is a multi index hence the slice on [0]
    elif type(df.columns) is pd.MultiIndex:
        if any(" " in c[0] for c in df):
            warnings.warn(
                "The dataframe contains columns with names that has white spaces. These would be converted to underscores while writing to the database."
            )
            df.columns = [c[0].replace(" ", "_") for c in df]

    if " " in tablename:
        warnings.warn(
            "The `tablename` has white spaces. These would be converted to underscores while writing to the database."
        )
        tablename = tablename.replace(" ", "_")

    # check if any column contains a boolean (bug on pd.io.sql.get_schema())
    bools = {c: "Boolean" for c in df.columns[df.dtypes == bool]}

    # create a create table SQL query
    query = pd.io.sql.get_schema(frame=df, name=f"{schemaname}.{tablename}", dtype=bools)

    # Format the query string to remove quotes and next line
    query = query.replace(f'"{schemaname}.{tablename}"', f'"{schemaname}".{tablename}')
    query = query.replace("\n", "")

    if drop_ifexists:
        raise NotImplementedError(
            f"""Cannot drop table if it exists due to security issues. Use ```pgIO.commitData('DROP TABLE IF EXISTS "{schemaname}".{tablename}, dbName="{dbname}"')``` to explicitly drop tables before using the write_db function'"""
        )
        # print("Dropping table with the same table name if it exists!")
        # pgIO.commitData(f'DROP TABLE IF EXISTS "{schemaname}".{tablename}', dbName={dbname})

    # Create a table on the database
    pgIO.commitData(query, dbName=dbname)

    # Insert values into the table
    query = sql.SQL("""INSERT into {}.{} values %s """).format(sql.Identifier(schemaname), sql.Identifier(tablename))
    r = pgIO.commitDataList(query, df.values.tolist(), dbName="cdm")

    if r and change_user:
        # fill up query
        alter_schema = sql.SQL("ALTER SCHEMA {schema_name} OWNER TO {change_user}").format(
            schema_name=sql.Identifier(schemaname), change_user=sql.Identifier(change_user)
        )

        alter_tables = sql.SQL("ALTER TABLE  {schema_name}.{table_name} OWNER TO {change_user}").format(
            schema_name=sql.Identifier(schemaname),
            table_name=sql.Identifier(tablename),
            change_user=sql.Identifier(change_user),
        )

        # commit query
        pgIO.commitData(alter_schema, dbName=dbname)
        pgIO.commitData(alter_tables, dbName=dbname)

        print(f"Transferred ownership to {change_user}..")
    return r


def _cohort_validator(cohort):
    """
    Helper function to validate cohorts passed to getter functions, and raises errors if the cohort does not mean certain conditions.
    Parameters
    ----------
    cohort:
        pandas DataFrame containing the following columns: ['person_id', 'start_date', 'end_date', 'cohort_id']
    Returns
    -------
    patients
        A NumPy array of patient IDs as type int
    """
    if cohort is None:
        raise ValueError("At least one of `patients` or `cohort` should be not None")
    if not isinstance(cohort, pd.DataFrame):
        raise TypeError("Cohort argument is to be a pandas dataframe")
    if sorted(cohort.columns.to_numpy()) != sorted(['person_id', 'start_date', 'end_date', 'cohort_id']):
        raise ValueError("Please ensure that cohort contains the columns ['person_id', 'start_date', 'end_date', 'cohort_id'] only")
    if all(isinstance(i, numbers.Integral) for i in cohort['person_id'].to_numpy()):
        patients = cohort['person_id'].to_numpy()
    else:
        raise TypeError("Please ensure that cohort `person_id` is of type int")
    if not (all(isinstance(i, np.datetime64) for i in cohort['start_date'].to_numpy()) and all(isinstance(i, np.datetime64) for i in cohort['end_date'].to_numpy())):
        raise (TypeError("Please ensure that cohort 'start_date' and 'end_date' are of type datetime"))
    return 