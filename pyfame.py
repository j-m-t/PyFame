"""A Python interface for Fame"""

from jnius import autoclass, JavaException
import pandas as pd
import re
import sys

__version__ = "0.1.3"


def checkstring(getinput):
    """
    Checks if input is a string or list.  If it is a string, it converts
    it to a one-observation list. If it is not a string, it leaves it as
    a list

    Args:
    getinput (list or str): The input that will be checked.

    Returns:
        list: The input in list format.

    """
    if isinstance(getinput, str):
        # Turn the string into a list
        getlist = [getinput]
    else:
        getlist = getinput
    return getlist


def findseries(dstore, series, database):
    """
    Checks for series names in the database and produces list of all possible
    series from database (if not declared beforehand).

    Args:
    dstore (jnius.reflect.com.fame.timeiq.persistence.DataStore): The Time IQ
        database object to be searched.
    series (list of str): List representation of series to find in the TimeIQ
        database.
    database (str): Name of FAME database, to be shown if we have an exception.

    Returns:
        list of str: A list of all possible series within the database, if no
            series were selected beforehand.

    """
    if series is None:
        series = []
        datainfo = dstore.matchWildCard("*")
        while datainfo.hasMoreElements():
            series.append(datainfo.nextElement().getName())
    else:
        # We need to check that the series that we want is part of the FAME
        # database, or else we will have a JavaException.
        # NOTE: If we end up having a series list that is empty, we will
        #       have to place checks such that we don't iterate over empty
        #       lists.
        reject = []
        for x in series:
            try:
                dstore.getTiqObjectCopy(x)
            except JavaException as error:
                message = str(error) + ' in' + database + '.db'
                print(message)
                reject.append(x)
        series = list(set(series) - set(reject))
    return series


def copyfreq(dstore, series):
    """
    Gathers frequency information for a series from its TimeIQ database object.

    Args:
    dstore (jnius.reflect.com.fame.timeiq.persistence.DataStore): The TimeIQ
        database object to be searched.
    series (str): Series name that will have their frequency checked.

    Returns:
        str: Frequency of the series (i.e. 'QUARTERLY').

    """
    copyfreq = dstore.getTiqObjectCopy(series)
    copyfreq = copyfreq.getObservations()
    copyfreq = copyfreq.frequency.toString()
    # TODO: This code has only been tested with quarterly frequencies.
    #       There is room to extend this to other (i.e. monthly or daily)
    #       frequencies as well.
    return copyfreq


def datecheck(getstart, getend):
    """
    This allows for getstart and getend to be YYYY values,
    converting them to YYYYQ1 and YYYYQ4 for getstart and getend
    dates, respectively.

    Args:
    getstart (str): The start date for the data, either in YYYY or
        YYYYQQ format.
    getend (str): The end date for the data, either in YYYY or
        YYYYQQ format.

    Returns:
        str: Start date in appropriate YYYYQQ format.
        str: End date in appropriate YYYYQQ format.

    """
    quartercheck = re.compile('[1-2][0-9]{3}$')
    if getstart is not None:
        if quartercheck.match(getstart) is not None:
            getstart = getstart + 'Q1'
    if getend is not None:
        if quartercheck.match(getend) is not None:
            getend = getend + 'Q4'
    # Check that getstart occurs before getend. If it doesn't, exit
    # and print error message.
    if (getstart is not None) and (getend is not None):
        if getstart > getend:
            sys.exit("pyfame.py: error: Dates are not in correct order.")
    # TODO: Give that only quarterly data is supported,
    # and that when this function is introduced we have yet to checked
    # indices for each series, we will do a simple conversion of YYYYQQ,
    # instead of create separate values for quarterly and monthly data.
    return getstart, getend


def calendar(observations):
    """
    Helper function which collects timestamp information
    for series from Fame database.

    Args:
        observations (jnius.reflect.com.fame.timeiq.data.ObservationList): The
            TimeIQ database object containing series information.

    Returns:
        str: First observation of the data in the series list.
        str: Last observation of the data in the series list.

    """
    cal = observations.getCalendar()
    first = cal.indexToString(observations.getFirstIndex())
    last = cal.indexToString(observations.getLastIndex())
    return first, last


def copyall(dstore, series):
    """
    Copies all the available data for a series in a Fame database. This
    function will be called by the class if both start and end dates are not
    provided. This function skips computing index values for the series
    since the getTiqObjectCopy method does not need these arguments.

    Args:
    dstore (jnius.reflect.com.fame.timeiq.persistence.DataStore): The TimeIQ
        database object where the data are stored.
    series (str): The series to be copied.

    Returns:
        list of float: All available data values for all series.
        list of str: A two-observation list of start and end date indices.

    """
    tiqobj = dstore.getTiqObjectCopy(series)
    obs = tiqobj.getObservations()
    copydata = obs.getValues().getDoubleArray()
    # Collect the TimeIQ index data for this series
    first, last = calendar(obs)
    getrange = [first, last]
    return copydata, getrange


def getindex(dstore, series, period, location):
    """
    Get time-index information for selected series if start and/or end dates
    are provided.

    Args:
    dstore (jnius.reflect.com.fame.timeiq.persistence.DataStore): The TimeIQ
        database object to be checked.
    series (str): The series that will have their time-index checked.
    period (str or None): The period that we will check.
    location (str): Either 'start' or 'end'; needed to get index info if period
        is 'None'.

    Returns:
        int: The time index according to period and location.

    """
    # Bring in DateHelper class from TimeIQ
    DH = autoclass('com.fame.timeiq.dates.DateHelper')
    # We check the date parameter to get the index values needed for
    # getTiqObjectCopy.
    if period is not None:
        # Convert the year information into integers
        year = int(period[:4])
        # Convert the quarterly date to the first day of the month.
        # Must be in YYYYQQ format.
        if str.upper(period[4:5]) == 'Q':
            month = (int(period[-1:])-1)*3+1
        # We don't use monthly data, but it may be offered...
        else:
            month = int(period[-2:])
        timeindex = DH.ymdToIndex(year, month)
    else:
        tiqobj = dstore.getTiqObjectCopy(series)
        obs = tiqobj.getObservations()
        if location is 'start':
            timeindex = obs.getFirstIndex()
        if location is 'end':
            timeindex = obs.getLastIndex()
    return timeindex


def copysome(dstore, series, getstart, getend):
    """
    Copies the data for a series in a selected FAME databse, as determined
    by the start and end dates provided.

    Args:
    dstore (jnius.reflect.com.fame.timeiq.persistence.DataStore): The TimeIQ
        database object where the data are stored.
    series (str): The series to be copied.
    getstart (str): The start date for the data, in YYYYQQ format.
    getend (str): The end date for the data, in YYYYQQ format.

    Returns:
        list of float: Selected data values for chosen series.
        list of str: A two-observation list of start and end date indices.

    """
    # TODO: There is some unnecessary duplication in this function and
    #       copyall - refactoring could be implemented.
    startindex = getindex(dstore, series, getstart, 'start')
    endindex = getindex(dstore, series, getend, 'end')
    # Create TimeIQ index
    index = [startindex, endindex]
    # Load the series data for our time range
    tiqobj = dstore.getTiqObjectCopy(series, index[0], index[1])
    obs = tiqobj.getObservations()
    # Copy over the data values
    copydata = obs.getValues().getDoubleArray()
    # Collect the TimeIQ index data for this series, to check whether
    # the dates we requested actually have data.
    first, last = calendar(obs)
    # Define conversion dictionaries for TimeIQ dates
    convert_q = {'Jan': 'Q1', 'Apr': 'Q2', 'Jul': 'Q3', 'Oct': 'Q4'}
    convert_m = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
                 'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
                 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'}
    # TODO: The rest of this function is so messy, there has to be a cleaner
    #       way of testing for all these conditions.
    # Convert TimeIQ dates from MmmYYYY to YYYYQQ format, after checking that
    # we have a proper index for this series.  If we don't, we force the series
    # to take the date we have selected.
    if first != 'INDEX_UNDEFINED':
        first = first[-4:]+convert_q[first[:3]]
    else:
        first = None
    if last != 'INDEX_UNDEFINED':
        last = last[-4:]+convert_q[last[:3]]
    else:
        last = None
    # In case we enter a date that is out of range for the series,
    # we change the date.  If the date entered was None, we replace it
    # with what was found by getindex().
    if getstart is not None:
        if (getstart < first):
            if first is not None:
                getstart = first
    else:
        if first is not None:
            getstart = first
    if getend is not None:
        if (getend > last):
            if last is not None:
                getend = last
    else:
        if last is not None:
            getend = last
    getrange = [getstart, getend]
    return copydata, getrange


def makedataframe(seriesdata, seriesname, getrange, getfreq):
    """
    Converts the series data list into indexed Pandas dataframe.

    Args:
    seriesdata (list of float): Data values for chosen series.
    seriesname (str): Data series name used for the column header.
    getrange (list of str): A two-observation list of start and end date
        indices.
    getfreq (str): Single character string used to index the dataframe
        in the period_range function.

    Returns:
        pandas.core.frame.DataFrame: Indexed dataframe for chosen series.
        list of str: A two-observation list of start and end date indices.

    """
    periodrange = pd.period_range(getrange[0],
                                  getrange[1],
                                  freq=getfreq)
    if seriesdata == []:
        variable = pd.DataFrame(index=periodrange, columns=seriesname.split())
    else:
        variable = pd.DataFrame(seriesdata)
        variable.columns = seriesname.split()
    variable = variable.set_index(periodrange)
    getrange - [str(variable.iloc[0].name), str(variable.iloc[-1].name)]
    return variable, getrange


class getfame:
    def __init__(self, getfiles, getseries=None,
                 getstart=None, getend=None,
                 getnames=None):
        """
        A Python interface to load FAME data.

        Args:
        getfiles (list or str): Complete filepath to FAME database or
            a list of filepaths. Necessary to open a database.
        getseries (list or str): A series name, or a list of series names.
            If None, all series in database are loaded.
        getstart (str): The series start date. If None, the first existing
            date is loaded.
        getend (str): The series end date. If None, the last existing date
            is loaded.
        getnames (list or str): A filename mnemonic, or a list of mnemonics.
            Helps if multiple Fame databases with similar or identical names
            are loaded.

        """
        # Convert single filename to a list if introduced as a single string.
        # This won't work if multiple filenames are introduced in one string.
        getfiles = checkstring(getfiles)
        self.getpaths = [x for x in getfiles]
        if getseries is not None:
            getseries = checkstring(getseries)
        # We set the names that will be used for dictionary keys.
        # Note that this will not be applied if we are onky looking at
        # one database.
        # If names are provided in the 'getnames' argument, we use them.
        if getnames is not None:
            self.getnames = checkstring(getnames)
        # If a list of unique names is not provided, we check to see if there
        # are non-unique names. If there are non-unique names, we arbitrarily
        # assign number identifiers to them.
        else:
            filenames = [x.split('/')[-1].split('.')[0] for x in self.getpaths]
            namecount = {x: filenames.count(x) for x in list(set(filenames))}
            # An ugly loop to assigne unique names to non-unique files:
            for key, val in namecount.items():
                if val > 1:  # We do not modify singletons in 'filenames'
                    count = 0
                    for i, x in enumerate(filenames):
                        if key == filenames[i]:
                            filenames[i] = filenames[i]+str(count)
                            count += 1
            self.getnames = filenames
        # Initialize FAME session
        # Class server: Singleton server object that is the starting point
        #               of all TimeIQ access
        server = autoclass('com.fame.timeiq.persistence.Server')
        instance = server.getInstance()
        session = instance.getSession()
        connection = session.createConnection(None)
        # Load FAME databases in a list:
        dstore = [connection.getDataStore(x) for x in self.getpaths]
        # Get series from FAME database, if series were not chosen beforehand:
        series = {x: findseries(dstore[i], getseries, x)
                  for i, x in enumerate(self.getnames)}
        # Make a list of the series frequencies:
        getfreq = {x: [(y, copyfreq(dstore[i], y))
                       for y in series[x]]
                   for i, x in enumerate(self.getnames) if len(series[x]) > 0}
        # To allow year-only getstart and getend arguments, do the following:
        getstart, getend = datecheck(getstart, getend)
        # Declare two empty dictionaries for the data and the period ranges:
        rawdata, limit = {}, {}
        # If start and end dates for the FAME database are not provided,
        # then we download all the data in the database.
        if (getstart is None) and (getend is None):
            for i, x in enumerate(self.getnames):
                if len(series[x]) > 0:
                    rawdata[x], limit[x] = zip*([copyall(dstore[i], y)
                                                 for y in series[x]])
        # Otherwise we download the data according to the start and end dates
        # that are provided:
        else:
            for i, x in enumerate(self.getnames):
                if len(series[x]) > 0:
                    rawdata[x], limit[x] = zip(*[copysome(dstore[i], y,
                                                          getstart, getend)
                                                 for y in series[x]])
        # Convert raw data into labeled and indexed Pandas dataframes, and
        # generate list of start and end dates for each series (since they
        # aren't always the same).
        for x in self.getnames:
            if len(series[x]) > 0:
                rawdata[x], limit[x] = zip(*[makedataframe(rawdata[x][i], y,
                                                           limit[x][i],
                                                           getfreq[x][i][1][0:1])
                                             for i, y in enumerate(series[x])])
        # Create instance objects
        self.getseries = series
        self.getrange = limit
        self.getfreq = getfreq
        # Concatenate individual dataframes
        if len(self.getnames) == 1:
            # Create a single dataframe if a single FAME database in provided:
            if rawdata != {}:
                self.data = pd.concat(rawdata[self.getnames[0]], axis=1)
            else:
                self.data = 'No data available for given query.'
        else:
            # Create a dictionary of dataframes if multiple FAME databases are
            # provided:
            self.data = {x: pd.concat(rawdata[x], axis=1)
                         for x in self.getnames if x in rawdata.keys()}
            self.compare(self.data, self.getnames, self.getseries)
        # Close data connection:
        for x in dstore:
            x.close()

    def compare(self, data, getnames, getseries):
        """
        Compares a series from different dataframes within its own dataframe.

        Args:
        data (dict): The dictionary containing all the dataframes.
        getnames (list): The list of mnemonics for the dataframes.
        getseries (dict): The series to be compared.

        """
        self.compare = {}
        # We find the intersection of series names that exist in our dict
        # of series names (where the keys are the database names).
        # We will only apply the compare method to those series which exist
        # in all (and not some) of the databases we are loading.
        getseries = set.intersection(*[set(getseries[x]) for x in getnames])
        for y in getseries:
            self.compare[y] = pd.concat([pd.DataFrame(data[x][y])
                                         for x in getnames], axis=1)
            self.compare[y].columns = [x for x in getnames]
