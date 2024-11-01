using System.Collections.Concurrent;
using System.Diagnostics;
using System.Management;
using System.Text;

public class Sorter
{
    static async Task Main(string[] args)
    {
        var timer = new Stopwatch();
        timer.Start();

        string inputFilePath = "testfile.txt"; // Input file path
        string outputFilePath;

        // Check if an output file path was provided as an argument
        if (args.Length < 1)
        {
            // Use a default output file path in the current directory
            outputFilePath = Path.Combine(Environment.CurrentDirectory, "sortedfile.txt");
            Console.WriteLine($"No output file path provided. Using default: {outputFilePath}");
        }
        else
        {
            outputFilePath = args[0];
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

    // Method to validate the output file path
    static bool ValidateOutputFilePath(string path)
    {
        var directory = Path.GetDirectoryName(path);

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
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);
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
        var sortedLines = lines.Select(line =>
        {
            var parts = line.Split(new[] { ". " }, 2, StringSplitOptions.None);
            return new
            {
                Number = int.Parse(parts[0]),
                Text = parts[1]
            };
        })
        .OrderBy(x => x.Text) // Sort by text first
        .ThenBy(x => x.Number) // Then sort by number
        .Select(x => $"{x.Number}. {x.Text}") // Reformat back to original format
        .ToList();

        string tempFileName = Path.GetTempFileName();

        await File.WriteAllLinesAsync(tempFileName, sortedLines);

        return tempFileName;
    }

    static void MergeSortedFiles(List<string> tempFiles, string outputFilePath)
    {
        using (var writer = new StreamWriter(outputFilePath))
        {
            try
            {
                var readers = tempFiles.Select(file => new StreamReader(file)).ToList();

                // Use a list to manage the heap
                var minHeap = new List<(int Number, string Text, int FileIndex)>();

                // Initialize the heap with the first line of each file
                for (int i = 0; i < readers.Count; i++)
                {
                    if (!readers[i].EndOfStream)
                    {
                        var line = readers[i].ReadLine();
                        var parts = line.Split(new[] { ". " }, 2, StringSplitOptions.None);

                        try
                        {
                            int number = int.Parse(parts[0]);
                            string text = parts[1];

                            minHeap.Add((number, text, i));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Error processing line from file {i}: {line}. Exception: {ex.Message}");
                        }
                    }
                }

                // Create a method to maintain the heap property
                void Heapify()
                {
                    minHeap.Sort((x, y) =>
                    {
                        int textComparison = x.Text.CompareTo(y.Text);
                        if (textComparison == 0)
                        {
                            return x.Number.CompareTo(y.Number);
                        }
                        return textComparison;
                    });
                }

                // Build the initial heap
                Heapify();

                while (minHeap.Count > 0)
                {
                    var minItem = minHeap[0]; // Get the smallest item
                    writer.WriteLine($"{minItem.Number}. {minItem.Text}");
                    minHeap.RemoveAt(0); // Remove the smallest item

                    // Read the next line from the same file as the minimum item
                    if (!readers[minItem.FileIndex].EndOfStream)
                    {
                        var line = readers[minItem.FileIndex].ReadLine();

                        var parts = line.Split(new[] { ". " }, 2, StringSplitOptions.None);

                        try
                        {
                            int number = int.Parse(parts[0]);
                            string text = parts[1];

                            minHeap.Add((number, text, minItem.FileIndex));
                            Heapify(); // Re-heapify after adding new item
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Error processing line from file {minItem.FileIndex}: {line}. Exception: {ex.Message}");
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