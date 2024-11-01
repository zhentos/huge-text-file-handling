public class Program
{
    static void Main(string[] args)
    {
        // Validate command line arguments and extract parameters
        string? fileName = null;
        ulong numberOfLines = 0;
        var filePath = Directory.GetCurrentDirectory(); // Default to current directory

        if (args.Length < 4 || !ParseArguments(args, ref fileName, ref numberOfLines, ref filePath))
        {
            ShowUsageMessage();
            return;
        }

        // Ensure the filename has a .txt extension
        if (!fileName.EndsWith(".txt", StringComparison.OrdinalIgnoreCase))
        {
            Console.WriteLine("Note: The '.txt' extension will be added automatically.");
            fileName += ".txt";
        }

        // Combine the file path and file name to get the full output path
        string fullFilePath = Path.Combine(filePath, fileName);

        // Generate the test file
        GenerateTestFile(fullFilePath, numberOfLines);
    }

    static bool ParseArguments(string[] args, ref string fileName, ref ulong numberOfLines, ref string filePath)
    {
        for (int i = 0; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "-f":
                    if (i + 1 < args.Length)
                    {
                        fileName = args[++i];
                    }
                    else
                    {
                        Console.WriteLine("Error: Filename is missing after -f flag.");
                        return false;
                    }
                    break;

                case "-ln":
                    if (i + 1 < args.Length && ulong.TryParse(args[++i], out ulong lines) && lines > 0)
                    {
                        numberOfLines = lines;
                    }
                    else
                    {
                        Console.WriteLine("Error: Number of lines must be a positive integer after -ln flag.");
                        return false;
                    }
                    break;

                case "-p":
                    if (i + 1 < args.Length)
                    {
                        filePath = args[++i];
                    }
                    break;

                default:
                    Console.WriteLine($"Warning: Unrecognized argument '{args[i]}'.");
                    break;
            }
        }

        return !string.IsNullOrEmpty(fileName) && numberOfLines > 0;
    }

    static void ShowUsageMessage()
    {
        Console.WriteLine("Please run the program providing mandatory parameters <filename> and <numberOfLinesInTheOutputFile>. If you wish you can provide [pathToTheOutputFile] as not required parameter.");
        Console.WriteLine("Example: FileGeneratorUtility.exe -f testFile -ln 10000 -p 'D:\\Temp'");
    }

    static void GenerateTestFile(string filePath, ulong numberOfLines)
    {
        var random = new Random();
        string[] strings = { "BMW", "Audi", "Mercedes", "Renault", "Ford", "BMW", "Audi" };

        using (var writer = new StreamWriter(filePath))
        {
            for (ulong i = 0; i < numberOfLines; i++)
            {
                var number = random.Next(1, 100000); // Random number
                string car = strings[random.Next(strings.Length)]; // Random car from the array
                writer.WriteLine($"{number}. {car}");
            }
        }

        Console.WriteLine($"Test file '{filePath}' has been created with {numberOfLines} lines.");
    }
}