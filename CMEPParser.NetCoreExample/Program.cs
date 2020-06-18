using System;

namespace CMEPParser.NetCoreExample
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");
            CMEPImporter  import = new CMEPImporter();
            string inputFolder = @"C:\Users\craig\source\repos\CMEPParser.NetCoreExample\CMEPParser.NetCoreExample\CMEPInputFiles";
            string outPutFile = @"C:\Users\craig\source\repos\CMEPParser.NetCoreExample\CMEPParser.NetCoreExample\CSVOutPutFiles\test.csv";
            import.Import(inputFolder, outPutFile);
        }
    }
}
