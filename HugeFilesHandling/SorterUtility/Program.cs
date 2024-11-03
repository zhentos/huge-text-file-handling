using SorterUtility;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Management;
using System.Text;

public class Program
{
    static async Task Main(string[] args)
    {
        var timer = new Stopwatch();
        timer.Start();

        string? inputFilePath = null;
        string outputFilePath = Path.Combine(Environment.CurrentDirectory, "sortedfile.txt"); // Default output file path

        // Parse command-line arguments
        for (int i = 0; i < args.Length; i++)
        {
            if (args[i] == "-in" && i + 1 < args.Length)
            {
                inputFilePath = args[i + 1]; // Get the input file path
                i++; // Skip the next argument since it's already processed
            }
            else if (args[i] == "-out" && i + 1 < args.Length)
            {
                outputFilePath = args[i + 1]; // Get the output file path
                i++; // Skip the next argument since it's already processed
            }
        }

        // Validate that an input file path was provided
        if (string.IsNullOrEmpty(inputFilePath))
        {
            Console.WriteLine("Error: Input file path is required. Use -in <inputfile>.");
            return;
        }

        // Validate the input file path
        if (!File.Exists(inputFilePath))
        {
            Console.WriteLine($"Error: The specified input file '{inputFilePath}' does not exist.");
            return;
        }

        // Validate the output file path
        if (!ValidateOutputFilePath(outputFilePath))
        {
            Console.WriteLine($"The specified output file path '{outputFilePath}' is invalid or the directory does not exist.");
            return;
        }

        int optimalChunkSize = CalculateOptimalChunkSize(inputFilePath, 0.5); // Allow 50% of available memory

        Console.WriteLine($"Sort process has been started at: {DateTime.Now.ToShortTimeString()}");
        Console.WriteLine($"Optimal Chunk Size is: {optimalChunkSize}");

        await SortFile(inputFilePath, outputFilePath, optimalChunkSize);

        timer.Stop();
        TimeSpan timeTaken = timer.Elapsed;
        var total = "Sort process has been ended successfully. Total time to process data: " + timeTaken.ToString(@"m\:ss\.fff");
        Console.WriteLine(total);
    }

    // Method to validate the output file path remains unchanged
    static bool ValidateOutputFilePath(string path)
    {
        string directory = Path.GetDirectoryName(path);

        // Check if the directory exists
        if (directory != null && !Directory.Exists(directory))
        {
            return false; // Directory does not exist
        }

        // Check if we can create or overwrite the file
        try
        {
            using (var fs = File.Create(path)) { }
            File.Delete(path); // Clean up after testing
            return true; // Valid path
        }
        catch (Exception)
        {
            return false; // Invalid path or unable to create file
        }
    }

    // Get the total memory in bytes
    static long GetTotalPhysicalMemory()
    {
        long totalMemory = 0;

        // Create a ManagementObjectSearcher to query WMI
        var searcher = new ManagementObjectSearcher("SELECT TotalPhysicalMemory FROM Win32_ComputerSystem");

        foreach (ManagementObject obj in searcher.Get())
        {
            // Retrieve the total physical memory
            totalMemory = Convert.ToInt64(obj["TotalPhysicalMemory"]);
        }

        return totalMemory; // Return the total memory in bytes
    }

    static int CalculateOptimalChunkSize(string filePath, double memoryFraction)
    {
        var totalMemory = GetTotalPhysicalMemory(); // Total physical memory in bytes

        long allowedMemory = (long)(totalMemory * memoryFraction); // Allow a fraction of total memory

        // Read a sample of lines to estimate memory usage
        const int sampleSize = 1000; // Number of lines to read for estimation
        var sampleLines = new List<string>();

        using (var reader = new StreamReader(filePath))
        {
            for (int i = 0; i < sampleSize && !reader.EndOfStream; i++)
            {
                sampleLines.Add(reader.ReadLine());
            }
        }

        // Estimate average line size in bytes
        long totalSize = sampleLines.Sum(line => Encoding.UTF8.GetByteCount(line));
        double averageLineSize = (double)totalSize / sampleLines.Count;

        // Calculate optimal chunk size based on allowed memory and average line size
        int optimalChunkSize = (int)(allowedMemory / averageLineSize);

        return optimalChunkSize > 0 ? optimalChunkSize : 1; // Ensure at least 1 line is returned
    }

    static async Task SortFile(string inputFilePath, string outputFilePath, int chunkSize)
    {
        try
        {
            var tempFiles = new ConcurrentBag<string>();
            var timer = new Stopwatch();
            timer.Start();

            // Step 1: Read in chunks and sort them
            using (var reader = new StreamReader(inputFilePath))
            {
                var currentChunk = new StringBuilder();

                //Note:I've been trying to play with buffer size, but it can't lead to performance improvement yet
                char[] buffer = new char[1024]; // Read in larger chunks (1 KB)

                int bytesRead;

                while ((bytesRead = await reader.ReadAsync(buffer, 0, buffer.Length)) > 0)
                {
                    currentChunk.Append(buffer, 0, bytesRead);

                    while (true)
                    {
                        int newlineIndex = currentChunk.ToString().IndexOf('\n');
                        if (newlineIndex == -1) break; // No complete line found

                        string line = currentChunk.ToString().Substring(0, newlineIndex).Trim(); // Trim whitespace
                        currentChunk.Remove(0, newlineIndex + 1); // Remove processed line

                        // Only add non-empty lines to temporary storage for sorting
                        if (!string.IsNullOrWhiteSpace(line))
                        {
                            tempFiles.Add(line);
                        }
                    }
                }

                // Process any remaining lines in the current chunk
                if (currentChunk.Length > 0)
                {
                    string remainingLine = currentChunk.ToString().Trim(); // Trim whitespace
                    if (!string.IsNullOrWhiteSpace(remainingLine))
                    {
                        tempFiles.Add(remainingLine); // Add only if it's not empty
                    }
                }
            }

            // Split lines into chunks for parallel processing
            var lineChunks = tempFiles.Select((line, index) => new { line, index })
                                       .GroupBy(x => x.index / chunkSize)
                                       .Select(g => g.Select(x => x.line).ToList())
                                       .ToList();

            // Sort and save chunks in parallel
            var sortTasks = lineChunks.Select(SortAndSaveChunk);
            var sortedTempFiles = await Task.WhenAll(sortTasks);

            timer.Stop();
            Console.WriteLine($"SortAndSaveChunk functions Time taken: {timer.Elapsed:mm\\:ss\\.fff}");

            var timerMerge = new Stopwatch();
            timerMerge.Start();

            // Step 2: Merge sorted chunks
            MergeSortedFiles(sortedTempFiles.ToList(), outputFilePath);

            timerMerge.Stop();
            Console.WriteLine($"MergeSortedFiles Time taken: {timerMerge.Elapsed:mm\\:ss\\.fff}");

            Console.WriteLine($"Sorted file '{outputFilePath}' created.");
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);
        }
    }

    static async Task<string> SortAndSaveChunk(List<string> lines)
    {
        int count = lines.Count;
        var sortedLines = new (int Number, string Text)[count];

        // Populate the array with parsed data
        for (int i = 0; i < count; i++)
        {
            var parts = lines[i].Split(new[] { ". " }, 2, StringSplitOptions.None);
            if (parts.Length == 2 && int.TryParse(parts[0], out int number))
            {
                sortedLines[i] = (number, parts[1]);
            }
        }

        // Sort the array using Array.Sort with a custom comparison
        Array.Sort(sortedLines, (x, y) =>
        {
            int textComparison = x.Text.CompareTo(y.Text);
            return textComparison == 0 ? x.Number.CompareTo(y.Number) : textComparison;
        });

        // Prepare the output file name
        string tempFileName = Path.GetTempFileName();

        // Write sorted lines to the temporary file
        using (var writer = new StreamWriter(tempFileName))
        {
            for (int i = 0; i < count; i++)
            {
                await writer.WriteLineAsync($"{sortedLines[i].Number}. {sortedLines[i].Text}");
            }
        }

        return tempFileName;
    }

    static void MergeSortedFiles(List<string> tempFiles, string outputFilePath)
    {
        using (var writer = new StreamWriter(outputFilePath))
        {
            try
            {
                var readers = tempFiles.Select(file => new StreamReader(file)).ToList();
                var minHeap = new MinHeap(); // Use a binary min-heap

                // Initialize the heap with the first line of each file
                for (int i = 0; i < readers.Count; i++)
                {
                    if (!readers[i].EndOfStream)
                    {
                        var line = readers[i].ReadLine();
                        var parts = line.Split(new[] { ". " }, 2, StringSplitOptions.None);

                        if (parts.Length == 2 && int.TryParse(parts[0], out int number))
                        {
                            minHeap.Insert((number, parts[1], i)); // Add to min-heap
                        }
                    }
                }

                while (minHeap.Count > 0)
                {
                    var minItem = minHeap.ExtractMin(); // Get and remove the smallest item
                    writer.WriteLine($"{minItem.Number}. {minItem.Text}");

                    // Read the next line from the same file as the minimum item
                    if (!readers[minItem.FileIndex].EndOfStream)
                    {
                        var line = readers[minItem.FileIndex].ReadLine();
                        var parts = line.Split(new[] { ". " }, 2, StringSplitOptions.None);

                        if (parts.Length == 2 && int.TryParse(parts[0], out int number))
                        {
                            minHeap.Insert((number, parts[1], minItem.FileIndex)); // Add to min-heap
                        }
                    }
                }

                foreach (var reader in readers)
                {
                    reader.Dispose();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception: {ex.Message}");
            }
        }
    }
}