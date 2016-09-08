package org.jboss.teiid;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkLoader {
    public static Options options;

    public static final String TEIID_JDBC_DRIVER = "org.teiid.jdbc.TeiidDriver";
    public static final String DEFAULT_TEIID_USER = "user";
    public static final String DEFAULT_TEIID_PASSWORD = "user";

    static {
        options = new Options();
        options.addOption("d", "driverClass", true, "driverClass");
        options.addOption("j", "jdbcUrl", true, "jdbc url to connect to");
        options.addOption("s", "sourceTable", true, "Fully qualified name of source table");
        options.addOption("u", "username", true, "Name of the source db user");
        options.addOption("p", "password", true, "Password of the source db user");
        options.addOption("t", "targetTable", true, "Name of target spark table");
        options.addOption("h", "help", false, "prints help");
        options.getOption("d").setOptionalArg(true);
        options.getOption("u").setOptionalArg(true);
        options.getOption("p").setOptionalArg(true);
    }

    private static CommandLine parseOptions(String[] args) throws ParseException {

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);
        return cmd;
    }

    public static void main(String[] args) throws ParseException {

        CommandLine cmd = parseOptions(args);

        System.out.println("Running command:" + cmd.toString());

        if (cmd.hasOption("h")) {
            help();
        }
        String jdbcClass = cmd.getOptionValue("d", TEIID_JDBC_DRIVER);
        String jdbcUrl = null;
        if (cmd.hasOption("j")) {
            jdbcUrl = cmd.getOptionValue("j");
        } else {
            System.err.println("JDBC url not provided, use option -j or --jdbcUrl");
            help();
        }
        String dbTable = null;
        if (cmd.hasOption("s")) {
            dbTable = cmd.getOptionValue("s");
        } else {
            System.err.println("Source table not provided, use option -s or --sourceTable");
            help();
        }
        String username = cmd.getOptionValue("u", DEFAULT_TEIID_USER);
        String password = cmd.getOptionValue("p", DEFAULT_TEIID_PASSWORD);
        String targetTable = null;
        if (cmd.hasOption("t")) {
            targetTable = cmd.getOptionValue("t");
        } else {
            System.err.println("Target table name not provided, use option -t or --targetTable");
            help();
        }
        try {
            Class.forName(jdbcClass);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        SparkSession spark = SparkSession.builder().appName("Spark loader").enableHiveSupport().getOrCreate();

        Map<String, String> options = new HashMap<String, String>();
        options.put("url", jdbcUrl);
        options.put("dbtable", dbTable);
        options.put("user", username);
        options.put("password", password);
        options.put("driver", jdbcClass);
        Dataset<Row> jdbcRow = spark.read().format("jdbc").options(options).load();
        StringBuilder b = new StringBuilder();
        for (String s : jdbcRow.columns()) {
            if (b.length() > 0)
                b.append(",");
            b.append(s);
        }
        String cols = b.toString();

        System.out.println("columns retreived:" + cols);

        jdbcRow.show();
        jdbcRow.select("*").write().saveAsTable(targetTable);

    }

    private static void help() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("SparkLoader", options);
        System.exit(0);
    }
}
