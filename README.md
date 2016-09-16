# ledama - LOFAR EoR DAta MAnagement

ledama contains all the code required for the:
- LOFAR EoR data management, processing and analysis. LEDAMA aims to provide tools (LModules) for the processing of a large number of measurement sets. These LModules need, as input, RefFiles. A RefFile is a file which contains the locations of measurement sets.

-LEDDB handling (for more information regarding the database see the LEDDB document), i.e. how to fill and browse the database. This also includes the tools for the diagnostic data visualization and the LEDDB web user interface (UI). The LModules for the diagnostic data visualization need, as input, DiagFiles. A DiagFile is a file with the references to diagnostic data stored in the LEDDB.

- Cluster monitoring. This is based on daemons running in each node of the LOFAR EoR cluster that are collecting network traffic, CPU, GPU (currently disabled), memory and disk usage information. There is a tool within the LEDDB web for the displaying of the collected data. There is also a LModule that can be used for that purpose.
