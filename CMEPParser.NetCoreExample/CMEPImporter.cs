using System;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Reflection;
//using ETSS_ImporterService.Model;

namespace ETSS_ImporterService
{
    class CMEPImporter
    {
        //DataAccessLayer _dal;

        //public CMEPImporter(DataAccessLayer dal)
        //{
        //    _dal = dal;
        //}

        public void Import(Import import)
        {
            try
            {
                DirectoryInfo di = new DirectoryInfo(@import.Path);
                FileInfo[] fi = di.GetFiles(import.FileSpec);

                // Process each file found with the import extension
                foreach (FileInfo fileInfo in fi)
                {
                    Debug.Print("Importing CMEP file: " + fileInfo.FullName, "Information");

                    string importLogId = null;

                    try
                    {
                        _dal.ImportLog_Insert(import.ImportSpecId,
                                              fileInfo.Name,
                                              Path.GetDirectoryName(fileInfo.FullName),
                                              (int)fileInfo.Length,
                                              Environment.UserName,
                                              Environment.MachineName,
                                              "CMEP",
                                              out importLogId);

                        ParseCMEP(fileInfo.FullName,
                                  importLogId,
                                  import.TableName);

                        if ((import.SprocName != null) && (import.SprocName != ""))
                            _dal.ExecuteImportSproc(import.SprocName,
                                                    importLogId);

                        Debug.Print(import.Name + " import completed.", "Information");

                        File.Move(fileInfo.FullName,
                                  Path.ChangeExtension(fileInfo.FullName, ".don"));

                        // update the import finish time
                        _dal.ImportLog_Update(importLogId, false, null);

                        string msg = "CMEP data file " + fileInfo.FullName + " imported. ImportLogId=" + importLogId;
                        try
                        {
                            //Util.SendAlert(msg, "ImportCMEPFile");
                        }
                        catch { }
                    }
                    catch (IOException ex)
                    {
                        if (ex.Message.Contains("being used by another process"))
                        {
                            Debug.Print("File " + fileInfo.FullName + " is locked by another process. Unable to import now.", "Warning");
                            _dal.ImportLog_Update(importLogId, true, ex.Message);
                        }
                        else
                        {
                            File.Move(fileInfo.FullName,
                                      Path.ChangeExtension(fileInfo.FullName, ".err"));
                            string msg = "CMEP import failed for file " + fileInfo.FullName + ", ImportLogId=" + importLogId + ". Error: " + ex.Message;
                            //Util.SendAlert(msg, "ImportCMEPFail");
                            throw new Exception(ex.Message);
                        }
                    }
                    catch (Exception ex)
                    {
                        File.Move(fileInfo.FullName,
                                  Path.ChangeExtension(fileInfo.FullName, ".err"));

                        _dal.ImportLog_Update(importLogId, true, ex.Message);

                        string msg = "CMEP import failed for file " + fileInfo.FullName + ", ImportLogId=" + importLogId + ". Error: " + ex.Message;
                        //Util.SendAlert(msg, "ImportCMEPFail");

                        throw new Exception(ex.Message);
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception(MethodBase.GetCurrentMethod().Name + " - " + ex.Message.ToString());
            }
        }

        public void ParseCMEP(string filePath, 
                              string importLogId,
                              string tableName)
        {
            // init the table that will hold the intervals.   
            DataTable cmepInterval = new DataTable();
            for (int i = 0; i < 29; i++)
            {
                cmepInterval.Columns.Add(new DataColumn());
            }

            using (StreamReader sr = File.OpenText(filePath))
            {
                int lineCtr = 0;
                int parentLine = 0;
                while (sr.Peek() >= 0)
                {
                    string line = sr.ReadLine();
                    string[] fields = line.Split(new Char[] { ',' });

                    for (int index = 0; index < fields.Length; index++)
                    {
                        // Remove beginning and ending quotes if they exist.  
                        if (fields[index].StartsWith("\""))
                            fields[index] = fields[index].TrimStart(new Char[] { '"' });
                        if (fields[index].EndsWith("\""))
                            fields[index] = fields[index].TrimEnd(new Char[] { '"' });
                        // convert empty fields to null
                        if (fields[index] == "")
                            fields[index] = null;
                    }

                    // Process the interval data records - "MEPMD01" - Metering Data Type 1 - Interval Data
                    if (fields[0] == "MEPMD01")
                    {
                        string readType = "I";
                        if (fields[10] != null)
                        {
                            // if uom contains REG then set readType to R else I
                            readType = (fields[10].ToUpper().Contains("REG")) ? "R" : "I";
                        }

                        lineCtr++;
                        if (readType == "R")
                            parentLine = lineCtr;

                        DateTime startReadDate = DateTime.ParseExact(fields[14],
                                                                     "yyyyMMddHHmm",
                                                                     CultureInfo.InvariantCulture);
                        startReadDate = DateTime.SpecifyKind(startReadDate, DateTimeKind.Utc);

                        // parse the interval triples 
                        // data triples - date time (utc),data qc,value
                        int intervalCtr = 0;
                        for (int index = 14; index < fields.Length; index += 3)
                        {
                            intervalCtr++;
                            DateTime readDate = new DateTime();

                            // the interval datatime is optional after the first interval. 
                            // if the dateTime is specified for the interval then use it else
                            // take the starting dateTime and calculate the dateTime for the interval
                            if (fields[index] != null)
                            {
                                readDate = DateTime.ParseExact(fields[index],
                                                               "yyyyMMddHHmm",
                                                               CultureInfo.InvariantCulture);
                                readDate = DateTime.SpecifyKind(readDate, DateTimeKind.Utc);
                            }
                            else
                            {
                                // interval end times are optional after the initial date/time so 
                                // calculate the next date/time.
                                if (fields[12] != null)
                                {
                                    readDate = CalcIntervalDateTime(startReadDate,
                                                                    fields[12],
                                                                    (intervalCtr - 1));
                                    readDate = DateTime.SpecifyKind(readDate, DateTimeKind.Utc);
                                }
                            }

                            string readQC = fields[index + 1];
                            string readValue = fields[index + 2];

                            // set the register read indicator.  Interval rows will be null
                            // first register read will be marked as the start and the 2nd interval 
                            // will be marked as the end.
                            string registerReadIndicator = null;
                            if (readType == "R")
                            {
                                if (index == 14)
                                    registerReadIndicator = "Start";
                                else if (index == 17)
                                    registerReadIndicator = "End";
                            }

                            // add record to table 
                            cmepInterval.Rows.Add(new string[] { importLogId, 
                                                                 readType, 
                                                                 fields[7], 
                                                                 fields[10], 
                                                                 readDate.ToString(), 
                                                                 readQC, 
                                                                 readValue,
                                                                 null,
                                                                 null,
                                                                 null,
                                                                 fields[0],
                                                                 fields[1],
                                                                 fields[2],
                                                                 fields[3],
                                                                 fields[4],
                                                                 fields[5],
                                                                 fields[6],
                                                                 fields[8],
                                                                 fields[9],
                                                                 fields[11],
                                                                 fields[12],
                                                                 null,
                                                                 registerReadIndicator,
                                                                 lineCtr.ToString(),
                                                                 parentLine.ToString(),
                                                                 null, null, null, null });
                        }

                        // Bulk load the table if we have 100k rows
                        if (cmepInterval.Rows.Count >= 100000)
                        {
                            _dal.BulkInsert(cmepInterval, tableName);
                            cmepInterval.Rows.Clear();
                        }
                    }
                    else if (fields[0] == "MEPMD02")  // TOU
                    {
                        string readType = "I";
                        if (fields[10] != null)
                        {
                            // if uom contains REG then set readType to R else I
                            readType = (fields[10].ToUpper().Contains("REG")) ? "R" : "I";
                        }

                        lineCtr++;
                        if (readType == "R")
                            parentLine = lineCtr;

                        // Convert the TOU Data timestamp to a DateTime field if it exists
                        DateTime touDataTimeStamp = new DateTime();
                        bool touDataTimeStampExists = false;

                        if (fields[14] != null)
                        {
                            try
                            {
                                touDataTimeStamp = DateTime.ParseExact(fields[14],
                                                                       "yyyyMMddHHmm",
                                                                       CultureInfo.InvariantCulture);
                                touDataTimeStamp = DateTime.SpecifyKind(touDataTimeStamp, DateTimeKind.Utc);
                                touDataTimeStampExists = true;
                            }
                            catch { }
                        }

                        // data triples - tou label, quality code, value
                        for (int index = 16; index < fields.Length; index += 3)
                        {
                            string touLabel = fields[index];
                            string readQC = fields[index + 1];
                            string readValue = fields[index + 2];

                            // add record to table 
                            cmepInterval.Rows.Add(new string[] { importLogId, 
                                                             readType, 
                                                             fields[7], 
                                                             fields[10], 
                                                             null, 
                                                             readQC, 
                                                             readValue,
                                                             touLabel,
                                                             fields[13],
                                                             (touDataTimeStampExists) ? touDataTimeStamp.ToString() : null,
                                                             fields[0],
                                                             fields[1],
                                                             fields[2],
                                                             fields[3],
                                                             fields[4],
                                                             fields[5],
                                                             fields[6],
                                                             fields[8],
                                                             fields[9],
                                                             fields[12],
                                                             null,
                                                             fields[11],
                                                             null,
                                                             lineCtr.ToString(),
                                                             parentLine.ToString(),
                                                             null, null, null, null });
                        }

                        // Bulk load the table if we have 100k rows
                        if (cmepInterval.Rows.Count >= 100000)
                        {
                            _dal.BulkInsert(cmepInterval, tableName);
                            cmepInterval.Rows.Clear();
                        }
                    }
                    else if (fields[0] == "MLA01")  // Alarm Report
                    {
                        //Increment line counter for each iteration
                        lineCtr++;

                        // Convert the data timestamp to a DateTime field if it exists
                        DateTime dataTimeStamp = new DateTime();
                        bool dataTimeStampExists = false;

                        if (fields[14] != null)
                        {
                            try
                            {
                                dataTimeStamp = DateTime.ParseExact(fields[14],
                                                                       "yyyyMMddHHmm",
                                                                       CultureInfo.InvariantCulture);
                                dataTimeStamp = DateTime.SpecifyKind(dataTimeStamp, DateTimeKind.Utc);
                                dataTimeStampExists = true;
                            }
                            catch { }
                        }

                        // data triples - time stamp, protocol text, alarm code
                        for (int index = 14; index < fields.Length; index += 3)
                        {
                            string timeStamp = fields[index];
                            string protocolText = fields[index + 1];
                            string alarmCode = fields[index + 2];

                            // add record to table 
                            cmepInterval.Rows.Add(new string[] { importLogId,   //ImportlogId
                                                             null,              //readType
                                                             fields[7],         //meterId
                                                             fields[10],        //uom
                                                             (dataTimeStampExists) ? dataTimeStamp.ToString() : null, //readDate
                                                             protocolText,      //readQC
                                                             alarmCode,         //readValue
                                                             null,              //touLabel
                                                             null,              //touDataStartTime
                                                             null,              //touDataTimestamp
                                                             fields[0],         //recordType
                                                             fields[1],         //recordVersion
                                                             fields[2],         //senderId
                                                             fields[3],         //senderCustomerId
                                                             fields[4],         //receiverId
                                                             fields[5],         //receiverCustomerId
                                                             fields[6],         //recordCreateDate
                                                             fields[8],         //purpose
                                                             fields[9],         //commodity
                                                             null,              //constant
                                                             null,              //timeInterval
                                                             null,              //season
                                                             null,              //RegisterReadIndicator
                                                             lineCtr.ToString(),//Line
                                                             parentLine.ToString(),
                                                             null, null, null, null }); //ParentLine
                        }

                        // Bulk load the table if we have 100k rows
                        if (cmepInterval.Rows.Count >= 100000)
                        {
                            _dal.BulkInsert(cmepInterval, tableName);
                            cmepInterval.Rows.Clear();
                        }
                    }
                    else if (fields[0] == "MEPEC01")  // Equipment Configuration
                    {
                        //Increment line counter for each iteration
                        lineCtr++;

                        int parameterCount = 0;
                        try
                        {
                            parameterCount = Convert.ToInt32(fields[21]);
                        }
                        catch { }

                        if (parameterCount > 0)
                        {
                            for (int index = 22; index < fields.Length; index += 2)
                            {
                                string parameterIdentifier = fields[index];
                                string parameterValue = fields[index + 1];

                                string purchaseDate = null;
                                if (!string.IsNullOrEmpty(fields[18]))
                                {
                                    purchaseDate = fields[18].Substring(0,4) + "-" + fields[18].Substring(4,2) + "-" + fields[18].Substring(6,2);
                                }

                                string installDate = null;
                                if (!string.IsNullOrEmpty(fields[19]))
                                {
                                    installDate = fields[19].Substring(0,4) + "-" + fields[19].Substring(4,2) + "-" + fields[19].Substring(6,2);
                                }

                                cmepInterval.Rows.Add(new string[] { importLogId, 
                                                                     "E", 
                                                                     fields[5], 
                                                                     null, 
                                                                     null, 
                                                                     null, 
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     fields[0],
                                                                     fields[1],
                                                                     fields[2],
                                                                     fields[3],
                                                                     fields[4],
                                                                     fields[5],
                                                                     fields[6],
                                                                     fields[9],
                                                                     fields[11],
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     installDate,
                                                                     purchaseDate,
                                                                     parameterIdentifier,
                                                                     parameterValue });
                            }
                        }

                        // Bulk load the table if we have 100k rows
                        if (cmepInterval.Rows.Count >= 100000)
                        {
                            _dal.BulkInsert(cmepInterval, tableName);
                            cmepInterval.Rows.Clear();
                        }
                    }
                }
            }

            // Bulk load any remaining rows
            if (cmepInterval.Rows.Count > 0)
            {
                _dal.BulkInsert(cmepInterval, tableName);
            }
        }

        /// <summary>
        /// Calculates a dateTime by taking a base startTime, time interval and the interval offset on the cmep row
        /// </summary>
        /// <param name="startDateTime"></param>
        /// <param name="interval"></param>
        /// <param name="intervalOffset"></param>
        /// <returns>interval DateTime</returns>
        private DateTime CalcIntervalDateTime(DateTime startDateTime,
                                              string interval,
                                              int intervalOffset)
        {
            // break apart the time interval string
            string intervalMinutes = interval.Substring(6, 2);
            string intervalHours = interval.Substring(4, 2);
            string intervalDays = interval.Substring(2, 2);

            // create a timespan
            TimeSpan ts = new TimeSpan(int.Parse(intervalDays),
                                       int.Parse(intervalHours),
                                       int.Parse(intervalMinutes),
                                       0);

            // keep adding the timespan until the offset is reached
            for (int intervalCtr = 0; intervalCtr < intervalOffset; intervalCtr++)
            {
                startDateTime = startDateTime.Add(ts);
            }
          
            return startDateTime;
        }
    }
}

