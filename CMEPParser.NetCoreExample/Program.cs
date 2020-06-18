using System;

namespace CMEPParser.NetCoreExample
{
    class Program
    {
        /// <summary>
        /// Imports a CMEP file and outputs a CSV file.
        /// When import is complete the CMEP file .dat is renamed to .don (for done).
        /// </summary>
        /// <param name="args"></param>
        static void Main(string[] args)
        {
            Console.WriteLine("Import file!");
            CMEPImporter  import = new CMEPImporter();
            string inputFolder = @"C:\Users\craig\source\repos\CMEPParser.NetCoreExample\CMEPParser.NetCoreExample\";
            string outPutFile = @"C:\Users\craig\source\repos\CMEPParser.NetCoreExample\CMEPParser.NetCoreExample\test.csv";
            import.Import(inputFolder, outPutFile);
            Console.WriteLine("ENTER btn To close!");
            Console.ReadLine();
        }
    }
}
