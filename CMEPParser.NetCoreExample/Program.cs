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
            Console.WriteLine("Import file App!");
            CMEPImporter  import = new CMEPImporter();
            string inputFolder = @"C:\Users\craig\source\repos\CMEPParser.NetCoreExample\CMEPParser.NetCoreExample\";
            string outPutFile = @"C:\Users\craig\source\repos\CMEPParser.NetCoreExample\CMEPParser.NetCoreExample\CSVOutPutFiles\test.csv";

            Console.WriteLine("Import file: " + inputFolder);
            Console.WriteLine("Import file: " + outPutFile);

            // Message to user
            Console.WriteLine("Enter to Run the parsing process");
            Console.ReadLine();

            // Run the parser
            import.Import(inputFolder, outPutFile);
            Console.WriteLine("ENTER to exit window!");
            Console.ReadLine();
        }
    }
}
