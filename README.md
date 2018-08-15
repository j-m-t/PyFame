# PyFame
A Python interface for FAME

This is a prototype Python package that interfaces directly to FAME through the TimeIQ Java toolkit, reading FAME databases and formatting them as Pandas dataframes.

Dependencies
------------
+ **[jnius](https://github.com/kivy/pyjnius)**
+ **[pandas](https://pandas.pydata.org/)**

Setup
-----
A few environment variables need to be defined in order for the class to work.  These should be set in your ```~/.bashrc``` file, or equivalent.

For Java - replace with the version of Java on your machine:
```bash
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
export PATH=/usr/lib/jvm/java-8-openjdk-amd64/bin:$PATH
```

For TimeIQ - The paths will depend on where TimeIQ is installed on your machine:
```bash
export TIMEIQDIR=/path/to/fame/timeiq
export CLASSPATH=$CLASSPATH:$TIMEIQDIR:$TIMEIQDIR/lib/timeiq.jar:$TIMEIQDIR/lib/TimeIQLicense.jar:
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/path/to/fame/timeiq/lib/linux_x86/64
```

Finally, since the script hasn't been properly package, it is imported easily after doing the following:
```
export PYTHONPATH=$PYTHONPATH:/path/to/pyfame/
```

Caveats
-------
PyFame has been developed to serve the immediate purpose of reading and compare quarterly data from multiple FAME databases, but it may be extended to read data of other frequencies.  There is no intention at the current time to implement a class to write data.

Usage
-----
To be continued...
