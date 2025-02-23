// See https://aka.ms/new-console-template for more information
// Console.WriteLine("Hello, World!");


using io.github.toyota32k.media;

try {
    var path = args[0];
    var mfs = new MovieFastStart() { Notify = new Progress() };
    using var inStream = new FileStream(path, FileMode.Open, FileAccess.Read);
    if(args.Length > 1) {
        var outPath = args[1];
        if (await mfs.Process(inStream, new OutputFileFactory(outPath))) {
            Console.WriteLine("Converted.");
        } else {
            Console.WriteLine("Failed.");
        }
    }
    else {
        if (await mfs.Check(inStream, true)) {
            Console.WriteLine($"SLOW  {mfs.SourceStatus.SlowStart}");
            Console.WriteLine($"FREE  {mfs.SourceStatus.HasFreeAtoms}");
            Console.WriteLine($"ERROR {mfs.SourceStatus.Unsupported}");
        }
        else {
            Console.WriteLine("FAST start.");
        }
    }
}
catch (Exception ex) {
    Console.WriteLine($"Error: {ex.Message}");
}

class OutputFileFactory : MovieFastStart.IOutputStreamFactory {
    private string path;
    public OutputFileFactory(string path) {
        this.path = path;
    }

    public Stream Create() {
        return new FileStream(path, FileMode.Create, FileAccess.Write);
    }

    public void Delete() {
        try {
            File.Delete(path);
        }
        catch (Exception e) {
            Console.WriteLine($"Error: {e}");
        }
    }
}

class Progress : MovieFastStart.INotify {
    public void Error(string message) {
        Console.WriteLine($"Error: {message}");
    }

    public void Message(string message) {
        Console.WriteLine($"Info: {message}");
    }

    public void UpdateProgress(long current, long total, string? message) {
        if (total > 0 && message != null) {
            Console.WriteLine($"{message}: {current}/{total}");
        }
        else if (total > 0) {
            Console.WriteLine($"{current}/{total}");
        }
        else if (message != null) {
            Console.WriteLine($"{message}");
        }
    }

    public void Verbose(string message) {
        Console.WriteLine($"Verbose: {message}");
    }

    public void Warning(string message) {
        Console.WriteLine($"Warning: {message}");
    }
}

