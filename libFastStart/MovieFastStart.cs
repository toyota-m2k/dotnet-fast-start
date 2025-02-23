using io.github.toyota32k.toolkit.net;
using System.Diagnostics;
using System.Text;

namespace io.github.toyota32k.media;

public class MovieFastStart {
    #region Results

    /**
     * Source analysis results
     */
    public class Status {
        public bool SlowStart { get; set; } = false;
        public bool HasFreeAtoms { get; set; } = false;
        public bool Unsupported { get; set; } = false;
        public bool AlreadySuitable => !SlowStart && !HasFreeAtoms;

    }
    public Status SourceStatus { get; private set; } = new Status();
    public Exception? LastException { get; private set; } = null;
    public long OutputLength { get; private set; } = 0L;

    #endregion

    #region Options

    public INotify? Notify { get; set; } = null;
    public string TaskName { get; set; } = "";
    public bool RemoveFreeAtom { get; set; } = true;         // true: Attempt to remove FREE Atom even if MOOV is at the beginning / false: Only check if MOOV is at the beginning
    public LoggerEx Logger { get; set; } = new LoggerEx("MFS");

    #endregion

    #region Logger / Notification
    public interface INotify {
        void Message(string message);
        void Error(string message);
        void Warning(string message);
        void Verbose(string message);
        void UpdateProgress(long current, long total, string? message);
    }

    private string FMT(string message) {
        return TaskName.IsEmpty() ? message : $"[{TaskName}]-{message}";
    }
    private void VERBOSE(string message) {
        message = FMT(message);
        Notify?.Verbose(message);
        Logger.debug(message);
    }
    private void ERROR(string message) {
        message = FMT(message);
        Notify?.Error(message);
        Logger.error(message);
    }
    private void ERROR(Exception e) {
        ERROR(e.ToString());
    }
    private void WARNING(string message) {
        message = FMT(message);
        Notify?.Warning(message);
        Logger.warn(message);
    }
    private void MESSAGE(string message) {
        message = FMT(message);
        Notify?.Message(message);
        Logger.info(message);
    }
    public bool OutputProgressLog { get; set; } = false;
    private void PROGRESS(long current, long total, string? message) {
        if (message != null) {
            message = FMT(message);
        }
        Notify?.UpdateProgress(current, total, message);
        if (OutputProgressLog) {
            if (message != null) {
                Logger.info($"{message}: {current} / {total}");
            }
            else {
                Logger.info($"{current} / {total}");
            }
        }
    }
    #endregion

    #region Internal Byteful Operations

    private const int CHUNK_SIZE = 8192;

    private static async Task<bool> read(Stream inputStream, byte[] buffer) {
        if (await inputStream.ReadAsync(buffer) != buffer.Length) {
            return false;
        }
        return true;
    }

    private static uint toUInt(byte[] buffer) {
        uint n = 0;
        foreach (var b in buffer) {
            n = (n << 8) + ((uint)b & 0xFF);
        }
        return n;
    }
    private static ulong toULong(byte[] buffer) {
        ulong n = 0;
        foreach (var b in buffer) {
            n = (n << 8) + ((uint)b & 0xFF);
        }
        return n;
    }

    private static async Task<uint?> readInt(Stream inputStream) {
        var buffer = new byte[4];
        if (!await read(inputStream, buffer)) return null;
        return toUInt(buffer);
    }
    private static async Task<uint> readIntOrThrow(Stream inputStream) {
        var n = await readInt(inputStream);
        if (!n.HasValue) {
            throw new Exception("invalid length");
        }
        return n.Value;
    }

    private static async Task<ulong?> readLong(Stream inputStream) {
        var buffer = new byte[8];
        if (!await read(inputStream, buffer)) return null;
        return toULong(buffer);
    }

    private static async Task<ulong> readLongOrThrow(Stream inputStream) {
        var n = await readLong(inputStream);
        if (!n.HasValue) {
            throw new Exception("invalid length");
        }
        return n.Value;
    }

    private static ASCIIEncoding encoding = new ASCIIEncoding();
    private static async Task<string?> readType(Stream inputStream) {
        var buffer = new byte[4];
        if (!await read(inputStream, buffer)) return null;
        return encoding.GetString(buffer);
    }

    private static byte[] toByteArray(uint n) {
        var b = new byte[4];
        for (var i = 0; i < 4; i++) {
            b[3 - i] = (byte)(n >> (8 * i));
        }
        Debug.Assert(n == toUInt(b));
        return b;
    }
    private static byte[] toByteArray(ulong n) {
        var b = new byte[8];
        for (var i = 0; i < 8; i++) {
            b[7 - i] = (byte)(n >> (8 * i));
        }
        Debug.Assert(n == toULong(b));
        return b;
    }

    #endregion

    #region Atom Processing

    private class Atom {
        public string type;
        public ulong size;
        public ulong start = 0;
        public Atom(string type, ulong size) {
            this.type = type;
            this.size = size;
        }
        public override string ToString() {
            return $"[{type}, start={start}, size={size}]";
        }
    }

    private class AtomList : List<Atom> {
        public Atom? moov { get; set; } = null;
        public Atom? mdat { get; set; } = null;
        public Atom? ftyp { get; set; } = null;

        public bool HasFtyp => ftyp != null;               // FTYP Atom exists
        public bool HasMoov => moov != null;               // MOOV Atom exists
        public bool HasMdat => mdat != null;               // MDAT Atom exists
        public bool IsValid => HasFtyp && HasMoov && HasMdat;   // File is valid (all required atoms exist)
        public bool MoovFirst { get; set; } = false;            // MOOV Atom is at the beginning
        public bool HasFreeAtoms { get; set; } = false;          // FREE Atoms exist
        public bool HasRedundantTail { get; set; } = false;     // There is redundant data after MDAT
        public bool Truncated { get; set; } = false;            // This file is truncated
        public long FreeSize { get; set; } = 0L;                // Total size of FREE Atoms before MDAT (used for offset calculation)

        // The stco/co64 contained in MOOV stores values to correct the offset of MDAT.
        // When MOOV or FREE is moved, it is necessary to correct this value.
        // This OffsetBias is the difference between the current offset and the offset after fast start / delete free.
        // In other words, by adding OffsetBias to the current offset, the corrected offset is obtained.
        public long OffsetBias => (MoovFirst ? 0 : (long)moov!.size) - FreeSize;
    }

    private async Task<Atom?> readAtom(Stream inputStream) {
        try {
            var size = await readInt(inputStream);
            if (size == null) return null;
            var type = await readType(inputStream);
            if (type == null) return null;
            if (size == 0) {
                ERROR($"Zero size atom at {inputStream.Position - 8} -- {type}");
                return null;
            }
            return new Atom(type, (uint)size);
        }
        catch (Exception) {
            return null;
        }
    }

    private async Task<AtomList> analyzeTopLevelAtoms(Stream inputStream) {
        var atomList = new AtomList();
        try {
            while (true) {
                // Read 8 bytes to get the size and type of the ATOM
                var atom = await readAtom(inputStream);
                if (atom == null) break;
                ulong skippedBytes = 8;
                if (atom.size == 1L) {
                    // For 64-bit size
                    atom.size = await readLongOrThrow(inputStream);
                    skippedBytes = 16;
                }
                atom.start = (ulong)inputStream.Position - skippedBytes;
                VERBOSE(atom.ToString());
                atomList.Add(atom);
                if (atom.size == 0L) break;

                switch (atom.type.ToLower()) {
                    case "ftyp":
                        atomList.ftyp = atom;
                        break;
                    case "moov":
                        if (!atomList.HasMoov) {
                            // First MOOV
                            atomList.moov = atom;
                            if (!atomList.HasMdat) {
                                atomList.MoovFirst = true;
                                if (!RemoveFreeAtom) {
                                    // Found after MDAT, mark as redundant tail
                                    return atomList;
                                }
                            }
                        }
                        else {
                            // Second MOOV (shouldn't happen, but might be leftover junk)
                            WARNING("Multiple moov atoms found.");
                            atom.type = "free"; // treat as free atom
                            atomList.HasFreeAtoms = true;
                            if (!atomList.HasMdat) {
                                // Record the size of FreeAtoms before MDAT (used for offset calculation)
                                atomList.FreeSize += (long)atom.size;
                            } else {
                                // Found after MDAT, mark as redundant tail
                                atomList.HasRedundantTail = true;
                            }
                        }
                        break;
                    case "mdat":
                        atomList.mdat = atom;
                        break;
                    case "free":
                        atomList.HasFreeAtoms = true;
                        if (!atomList.HasMdat) {
                            // Record the size of FreeAtoms before MDAT (used for offset calculation)
                            atomList.FreeSize += (long)atom.size;
                        }
                        break;
                    default:
                        break;
                }
                try {
                    inputStream.Position += ((long)atom.size - (long)skippedBytes);
                }
                catch (Exception e) {
                    ERROR("Cannot seek to next atom, maybe truncated.");
                    atomList.Truncated = true;
                    break;
                }
            }
        }
        catch (Exception e) {
            ERROR(e);
        }
        return atomList;
    }

    private async Task<Atom?> skipToNextTable(Stream inputStream) {
        while (true) {
            var atom = await readAtom(inputStream);
            if (atom == null) break;
            if (isTableType(atom.type)) {
                return atom;
            } else if (isKnownAncestorType(atom.type)) {
                continue;
            } else {
                // inputStream.Seek((long)atom.size - 8, SeekOrigin.Current);
                inputStream.Position += ((long)atom.size - 8);
            }
        }
        return null;
    }

    private static bool isKnownAncestorType(string type) {
        return
            type.Equals("trak", StringComparison.OrdinalIgnoreCase) ||
            type.Equals("mdia", StringComparison.OrdinalIgnoreCase) ||
            type.Equals("minf", StringComparison.OrdinalIgnoreCase) ||
            type.Equals("stbl", StringComparison.OrdinalIgnoreCase);
    }

    private static bool isTableType(string type) {
        return
        type.Equals("stco", StringComparison.OrdinalIgnoreCase) ||
        type.Equals("co64", StringComparison.OrdinalIgnoreCase);
    }

    #endregion

    public interface IOutputStreamFactory {
        Stream Create();

        // delete output file if failed
        void Delete();
    }

    /**
     * Core of Fast Start Processing
     * @return true: Converted / false: Not Converted
     */
    private async Task<bool> processImpl(
        Stream inputStream, IOutputStreamFactory? 
        outputStreamFactory) {
        OutputLength = 0L;

        MESSAGE($"Analyzing index of top level atoms...");
        var atomList = await analyzeTopLevelAtoms(inputStream);
        if (!atomList.IsValid) {
            // FTYPE/MOOV/MDAT are not all present
            MESSAGE($"Invalid file.");
            SourceStatus.Unsupported = true;
            return false;
        }

        SourceStatus.HasFreeAtoms = atomList.HasFreeAtoms;
        if (atomList.MoovFirst) {
            // MOOV is already at the beginning
            if (!atomList.HasFreeAtoms && !atomList.HasRedundantTail) {
                // No redundant atoms, so no conversion needed
                SourceStatus.HasFreeAtoms = atomList.HasFreeAtoms;
                MESSAGE($"File already suitable.");
                return false;
            }
            if (!RemoveFreeAtom) {
                // If not removing FREE atoms, no further processing is needed
                MESSAGE($"File has redundant atoms but ignored.");
                return false;
            }
        } else {
            // MOOV is not at the beginning
            SourceStatus.SlowStart = true;
        }


        if (outputStreamFactory == null) {
            // Check only
            MESSAGE($"File needs patching.");
            return true;
        }

        MESSAGE($"Patching moov...");
        var moov = atomList.moov!;  // Already checked by IsValid
        int offset = (int)atomList.OffsetBias;
        // Read MOOV contents to buffer;
        byte[] moovContents = new byte[(int)moov.size];
        try {
            //inputStream.Seek((long)moov.start, SeekOrigin.Begin);
            inputStream.Position = (long)moov.start;
            await inputStream.ReadAsync(moovContents);
        }
        catch (IOException ex) {
            Logger.error(ex);
            return false;
        }

        // collect offset of stco/co64 on moovContents
        using var moovIn = new MemoryStream(moovContents);
        using var moovOut = new MemoryStream(moovContents.Length);
        try {
            Atom? atom;
            moovIn.Position = 8;    // skip type and size
            while ((atom = await skipToNextTable(moovIn)) != null) {
                moovIn.Position += 4; //skip version and flags
                uint entryCount = await readIntOrThrow(moovIn);
                VERBOSE($"Patching {atom.type} with {entryCount} entries.");

                int entriesStart = (int)moovIn.Position;
                //write up to start of the entries
                moovOut.Write(moovContents, (int)moovOut.Length, entriesStart - (int)moovOut.Length);

                if (atom.type.Equals("stco", StringComparison.OrdinalIgnoreCase)) { //32 bit
                    for (int i = 0; i < entryCount; i++) {
                        var entry = toByteArray((uint)((int)await readIntOrThrow(moovIn) + offset));
                        moovOut.Write(entry);
                    }
                }
                else { //64 bit
                    for (int i = 0; i < entryCount; i++) {
                        var entry = toByteArray((ulong)((long)await readLongOrThrow(moovIn) + offset));
                        moovOut.Write(entry);
                    }
                }
            }

            if (moovOut.Length < moovContents.Length) { //write the rest
                moovOut.Write(moovContents, (int)moovOut.Length, moovContents.Length - (int)moovOut.Length);
            }

        }
        catch (Exception ex) {
            Logger.error(ex);
            return false;
        }


        MESSAGE("Writing output file:");
        long totalLength = 0L;
        try {
            using (var outputStream = outputStreamFactory.Create()) {
                // write ftype
                var ftyp = atomList.ftyp!;  // already checked by IsValid
                VERBOSE($"Writing ftyp at {outputStream.Position} length={ftyp.size}");

                inputStream.Position = (long)ftyp.start;
                var buffer = new byte[ftyp.size];
                if (!await read(inputStream, buffer)) {
                    throw new Exception("invalid ftyp");
                }
                await outputStream.WriteAsync(buffer);
                totalLength += (long)ftyp.size;

                // write patched moov
                VERBOSE($"Writing moov at {outputStream.Position} length={moovOut.Length}");
                moovOut.Position = 0;
                await moovOut.CopyToAsync(outputStream);
                totalLength += moovOut.Length;

                //write everything else! (maily mdat)
                foreach (var atom in atomList) {
                    if (atom.type.Equals("ftyp", StringComparison.OrdinalIgnoreCase) ||
                        atom.type.Equals("moov", StringComparison.OrdinalIgnoreCase) ||
                        atom.type.Equals("free", StringComparison.OrdinalIgnoreCase)) {
                        continue;
                    }
                    VERBOSE($"Writing {atom.type} at {outputStream.Position} length={atom.size}");

                    inputStream.Position = (long)atom.start;
                    byte[] chunk = new byte[CHUNK_SIZE];
                    long remain = (long)atom.size;
                    while (remain > 0) {
                        var len = await inputStream.ReadAsync(chunk, 0, (int)Math.Min(remain, CHUNK_SIZE));
                        if (len <= 0) {
                            ERROR($"Found Incomplete Atom: {atom.type} - {remain} bytes short.");
                            // input file is truncated
                            throw new Exception("Incomplete Atom: " + atom.type);
                        }
                        await outputStream.WriteAsync(chunk, 0, len);
                        remain -= len;
                        PROGRESS((long)atom.size - remain, (long)atom.size, null);
                    }
                    totalLength += (long)atom.size;
                }
                OutputLength = totalLength;
                await outputStream.FlushAsync();
            }
            MESSAGE("Write complete!");
            return true;
        }
        catch (Exception ex) {
            ERROR(ex);
            outputStreamFactory.Delete();
            return false;
        }
    }
    
    public async Task<bool> Process(Stream inputStream, IOutputStreamFactory outputStreamFactory) {
        LastException = null;
        try {
            return await processImpl(inputStream, outputStreamFactory);
        }
        catch (Exception ex) {
            ERROR(ex);
            return false;
        }
    }

    public async Task<bool> Check(Stream inputStream, bool removeFree = true) {
        LastException = null;
        try {
            return await processImpl(inputStream, null);
        }
        catch (Exception ex) {
            ERROR(ex);
            return false;
        }
    }
}
