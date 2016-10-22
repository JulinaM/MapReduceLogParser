import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by me on 10/22/16.
 */
public class LogMapper extends Mapper<LongWritable, Text, Text, Text> {
    private LogParser logParser = new LogParser();
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        if (line != null) {
            logParser.parse(line);
            Log log = logParser.getLog();
            context.write( new Text(log.ipAdress), new Text(log.request));
        }
    }


    private class LogParser {
        private String logEntryPattern = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"";
        private static final int NUM_FIELDS = 9;
        private Log log;

        private void parse(String logEntryLine){
            Pattern p = Pattern.compile(logEntryPattern);
            Matcher matcher = p.matcher(logEntryLine);
            if (!matcher.matches() || NUM_FIELDS != matcher.groupCount()) {
                System.err.println("Bad log entry (or problem with RE?):");
                System.err.println(logEntryLine);
                return;
            }
            this.log = new Log(matcher.group(1), matcher.group(4), matcher.group(5),
                    matcher.group(6), Integer.parseInt(matcher.group(7)), matcher.group(9));
        }

        private Log getLog() {
            return this.log;
        }

    }

    private class Log{
        private String ipAdress;
        private String dateAndTime;
        private String request;
        private String response;
        private int bytes;
        private String browser;

        Log(String ipAdress, String dateAndTime, String request, String response, int bytes, String browser){
            this.ipAdress = ipAdress;
            this.dateAndTime = dateAndTime;
            this.request = request;
            this.response = response;
            this.bytes = bytes;
            this.browser = browser;
        }
    }


    public static void main(String[] args) {
        String logEntryLine = "123.45.67.89 - - [27/Oct/2000:09:27:09 -0400] \"GET /java/javaResources.html HTTP/1.0\" 200 10450 \"-\" \"Mozilla/4.6 [en] (X11; U; OpenBSD 2.8 i386; Nav)\"";

        LogMapper logMapper = new LogMapper();
        logMapper.logParser.parse(logEntryLine);
        System.out.println(logMapper.logParser.getLog().ipAdress);
        System.out.println(logMapper.logParser.getLog().request);
        System.out.println(logMapper.logParser.getLog().request.split("\\s")[0].toUpperCase());
    }

}
