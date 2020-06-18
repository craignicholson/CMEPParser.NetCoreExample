# CMEP Parser Example Client Console App

Requirements
------------
Import a standard CMEP (California Meter Exchanged Protocol) file and output the results to a CSV file

Features
------------
TBD

Command Line Interface Example
------------
One can execute the application from the command line.  Here is an example  you can run from windows command prompt.

Windows
```
C:\> cd "C:\Users\CivilFilingClient\"
C:\Users\CivilFilingClient> CivilFilingClient.exe "888888005" "P@ssword" "https://dptng.njcourts.gov:2045/civilFilingWS_t" "C:\Files\TestCorp2Corp_MissingBranchID.xml" 

C:\Users\CivilFilingClient> CivilFilingClient.exe "888888005" "P@ssword" "https://dptng.njcourts.gov:2045/civilFilingWS_t" "C:\Files\TestIndivid2Individ.xml" 

```

Mac
```

```

Linux
```

```


Installation
------------
TBD

Dependencies
------------
* TBD

Application Setting (AppName.exe.config)
------------
NOT IMPLEMENTED

```xml

  <appSettings>
    <add key="cmepInputFolder" value="/////"/>
    <add key="csvOutputFolder" value="////"/
  </appSettings>

```

Test Files
--------------
* CMEP_Test_File.dat

Notes
-------------