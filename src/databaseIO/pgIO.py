import json
import psycopg2
import os
from psycopg2.extras import execute_values
import warnings


if "DB_PATH" in os.environ:
    dbpath = os.environ["DB_PATH"]
else:
    dbpath = "../config/db.json"

# monkey patch the warnings to suppress the output of warnings.warn(notice)
warnings.formatwarning = lambda message, *args, **kwargs: f"{message}\n"


def create_connection(dbName=None, conn_kwargs=None):
    """Create psycopg2 connection"""
    if conn_kwargs is not None:
        warnings.warn(
            "The use of `conn_kwargs` is discouraged. There could be a risk of exposing your password. Please ensure that it is sufficiently protected and proceed with caution!"
        )
        return psycopg2.connect(**conn_kwargs)

    """In case we are in a Dash environment, we obtain the credentials from env variables"""
    if os.environ.get("DASH_APP_NAME") is not None:
        params = {
            "user": os.environ["DATABASE_USER"],
            "password": os.environ["DATABASE_PASSWORD"],
            "database": "cdm",
            "host": os.environ["DATABASE_HOST"],
        }

        return psycopg2.connect(
            user=params["user"], password=params["password"], database=params["database"], host=params["host"]
        )

    with open(dbpath) as dbconfig:
        db = json.load(dbconfig)

    # Check whether a dbName is available
    if (dbName is None) and ("defaultDB" in db):
        dbName = db["defaultDB"]

    # Check whether a dbName has been specified
    if dbName is None:
        raise FileNotFoundError("A database name has not been specified.")

    return psycopg2.connect(db[dbName]["connection"])


def getAllData(query, values=None, dbName=None, return_colnames=False, conn_kwargs=None):
    """query data from the database
    Query the data over here. If there is a problem with the data, it is going
    to raise an exception. Note that the location of the databases are assumed to be present within the file ``../config/db.json``.
    Parameters
    ----------
    query : {str}
        The query to be made to the database
    values : {tuple or list-like}, optional
        Additional values to be passed to the query (the default is None)
    dbName : {str or None}, optional
        The name of the database to use. If this is None, the function will
        attempt to read the name from the ``defaultDB`` item within the
        file ``../config/db.json``.
    return_colnames: {bool}, optional
        Flag to return column names along with the values
    conn_kwargs: {dict}, optional
        Custom kwargs for creating psycopg2 connection.
    Returns
    -------
    list or None
        A list of tuples containing the values is returned. In case
        there is an error, an exception would be raised
    """

    vals = None

    try:
        conn = create_connection(dbName, conn_kwargs)
        cur = conn.cursor()
    except Exception:
        raise Exception("Unable to connect to the database")

    try:

        if values is None:
            cur.execute(query)
        else:
            cur.execute(query, values)

        # We assume that the data is small so we
        # can download the entire thing here ...
        # -------------------------------------------
        vals = cur.fetchall()
        if return_colnames:
            colnames = [desc[0] for desc in cur.description]
            vals = [vals]
            vals.append(colnames)

    except Exception:
        raise Exception("Unable to obtain data from the database for:\n query: {}\nvalues: {} ".format(query, values))

    try:
        # warn user if postgreSQL communicated any NOTICES to us with the last command executed e.g. DROP TABLE etc.
        for notice in conn.notices:
            warnings.warn("".join([notice, cur.statusmessage]))
        cur.close()
        conn.close()
    except Exception:
        raise Exception("Unable to disconnect to the database")

    return vals


def getDataIterator(query, values=None, chunks=100, dbName=None, conn_kwargs=None):
    """Create an iterator from a largish query
    This is a generator that returns values in chunks of chunksize ``chunks``.
    Parameters
    ----------
    query : {str}
        The query to be made to the database
    values : {tuple or list-like}, optional
        Additional values to be passed to the query (the default
        is None)
    chunks : {number}, optional
        This is the number of rows that the data is going to return at every call
        if __next__() to this function. (the default is 100)
    dbName : {str or None}, optional
        The name of the database to use. If this is None, the function will
        attempt to read the name from the ``defaultDB`` item within the
        file ``../config/db.json``.
    conn_kwargs: {dict}, optional
        Custom kwargs for creating psycopg2 connection.
    Yields
    ------
    list of tuples
        A list of tuples from the query, with a maximum of ``chunks`` tuples returned at one time. In case there is an error, an exception would be raised
    """

    try:
        conn = create_connection(dbName, conn_kwargs)
        cur = conn.cursor("remote")
    except Exception:
        raise Exception("Unable to connect to the database")

    try:

        if values is None:
            cur.execute(query)
        else:
            cur.execute(query, values)

        while True:
            vals = cur.fetchmany(chunks)
            if len(vals) == 0:
                break

            yield vals

    except Exception:
        raise Exception("Unable to obtain data from the database for:\n query: {}\nvalues: {}".format(query, values))

    try:
        # warn user if postgreSQL communicated any NOTICES to us with the last command executed e.g. DROP TABLE etc.
        for notice in conn.notices:
            warnings.warn("".join([notice, cur.statusmessage]))
        conn.close()
    except Exception:
        raise Exception("Unable to disconnect to the database")

    return


def getSingleDataIterator(query, values=None, dbName=None, conn_kwargs=None):
    """Create an iterator from a largish query
    This is a generator that returns values in chunks of chunksize 1.
    Parameters
    ----------
    query : {str}
        The query to be made to the database
    values : {tuple or list-like}, optional
        Additional values to be passed to the query (the default
        is None)
    dbName : {str or None}, optional
        The name of the database to use. If this is None, the function will
        attempt to read the name from the ``defaultDB`` item within the
        file ``../config/db.json``.
    conn_kwargs: {dict}, optional
        Custom kwargs for creating psycopg2 connection.
    Yields
    ------
    list of tuples
        A list of tuples from the query, with a maximum of ``chunks`` tuples returned at one time. In case there is an error, an exception would be raised.
    """

    try:
        conn = create_connection(dbName, conn_kwargs)
        cur = conn.cursor("remote")
    except Exception:
        raise Exception("Unable to connect to the database")

    try:

        if values is None:
            cur.execute(query)
        else:
            cur.execute(query, values)

        while True:
            vals = cur.fetchone()
            if vals is None:
                break

            yield vals

    except Exception:
        raise Exception("Unable to obtain data from the database for:\n query: {}\nvalues: {}".format(query, values))

    try:
        # warn user if postgreSQL communicated any NOTICES to us with the last command executed e.g. DROP TABLE etc.
        for notice in conn.notices:
            warnings.warn("".join([notice, cur.statusmessage]))
        conn.close()
    except Exception:
        raise Exception("Unable to disconnect to the database")

    return


def commitData(query, values=None, dbName=None, conn_kwargs=None):
    """query data from the database
    Query the data over here. If there is a problem with
    the data, it is going to raise an exception. Your program needs to check whether there was an error with the query by checking for a ``None``
    return value
    Parameters
    ----------
    query : {str}
        The query to be made to the database
    values : {tuple or list-like}, optional
        Additional values to be passed to the query (the default
        is None)
    dbName : {str or None}, optional
        The name of the database to use. If this is None, the function will
        attempt to read the name from the ``defaultDB`` item within the
        file ``../config/db.json``.
    conn_kwargs: {dict}, optional
        Custom kwargs for creating psycopg2 connection.
    Returns
    -------
    True or None
        On successful completion, a ``True`` is returned. In case there is an error, an exception would be raised
    """

    vals = True

    try:
        conn = create_connection(dbName, conn_kwargs)
        cur = conn.cursor()
    except Exception:
        raise Exception("Unable to connect to the database")

    try:

        if values is None:
            cur.execute(query)
        else:
            cur.execute(query, values)

    except Exception:
        raise Exception("Unable to obtain data from the database for:\n query: {}\nvalues: {}".format(query, values))

    try:
        conn.commit()
        # warn user if postgreSQL communicated any NOTICES to us with the last command executed e.g. DROP TABLE etc.
        for notice in conn.notices:
            warnings.warn("".join([notice, cur.statusmessage]))
        cur.close()
        conn.close()
    except Exception:
        raise Exception("Unable to disconnect to the database")

    return vals


def commitDataList(query, values, dbName=None, conn_kwargs=None):
    """query data from the database
    Query the data over here. If there is a problem with
    the data, it is going to raise an exception. Your program needs to check whether there was an error with the query by checking for a ``None``
    return value
    Parameters
    ----------
    query : {str}
        The query to be made to the database
    values : {tuple or list-like}, optional
        Additional values to be passed to the query (the default
        is None)
    dbName : {str or None}, optional
        The name of the database to use. If this is None, the function will
        attempt to read the name from the ``defaultDB`` item within the
        file ``../config/db.json``.
    conn_kwargs: {dict}, optional
        Custom kwargs for creating psycopg2 connection.
    Returns
    -------
    True or None
        A successful completion of this function returns a ``True``.
        In case there is an error, an exception would be raised.
    """

    val = True

    try:
        conn = create_connection(dbName, conn_kwargs)
        cur = conn.cursor()
    except Exception:
        raise Exception("Unable to connect to the database")

    try:
        query = cur.mogrify(query)
        execute_values(cur, query, values)
    except Exception:
        raise Exception("Unable to execute query for:\n query: {}\nvalues: {}".format(query, values))

    try:
        conn.commit()
        # warn user if postgreSQL communicated any NOTICES to us with the last command executed e.g. DROP TABLE etc.
        for notice in conn.notices:
            warnings.warn("".join([notice, cur.statusmessage]))
        cur.close()
        conn.close()
    except Exception:
        raise Exception("Unable to disconnect to the database")

    return 