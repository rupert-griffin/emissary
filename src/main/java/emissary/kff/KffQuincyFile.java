package emissary.kff;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * KffQuincyFile compares files against the Quincy KFF dataset. The dataset only contains MD5 sums to that's our
 * preferred algorithm, and gives a record length of 16 bytes with no CRC
 */
public class KffQuincyFile extends KffFile {
    /**
     * Create the Quincy Filter
     * 
     * @param filename name of fixed record length file to mmap
     * @param filterName name for this filter to report hits
     * @param ftype type of filter
     */
    public KffQuincyFile(String filename, String filterName, FilterType ftype) throws IOException {
        super(filename, filterName, ftype, 16);
        super.myPreferredAlgorithm = "MD5";
    }

    @SuppressWarnings("SystemOut")
    public static void main(String[] args) throws Exception {
        KffChain kff = new KffChain();
        KffFile kfile = new KffQuincyFile(args[0], "QUINCYTEST", FilterType.IGNORE);
        kff.addFilter(kfile);
        kff.addAlgorithm("CRC32");
        kff.addAlgorithm("MD5");
        kff.addAlgorithm("SHA-1");
        kff.addAlgorithm("SHA-256");

        for (int i = 1; i < args.length; i++) {
            try (InputStream is = Files.newInputStream(Paths.get(args[i]))) {
                byte[] buffer = IOUtils.toByteArray(is);

                KffResult r = kff.check(args[i], buffer);
                System.out.println(args[i] + ": " + r.isKnown() + " - " + r.getMd5String());
            }
        }
    }
}
