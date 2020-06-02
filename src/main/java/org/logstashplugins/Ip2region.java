package org.logstashplugins;

import co.elastic.logstash.api.*;
import org.lionsoul.ip2region.DataBlock;
import org.lionsoul.ip2region.DbConfig;
import org.lionsoul.ip2region.DbSearcher;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;


// class name must match plugin name
@LogstashPlugin(name = "ip2region")
public class Ip2region implements Filter {

    public static final PluginConfigSpec<String> DATABASE =
            PluginConfigSpec.stringSetting("database", "/data/ip2region/ip2region.db");
    public static final PluginConfigSpec<String> SOURCE_CONFIG =
            PluginConfigSpec.stringSetting("source", "ip");
    public static final PluginConfigSpec<String> TARGET_CONFIG =
            PluginConfigSpec.stringSetting("target", "region_code");

    private String id;
    private String sourceField;
    private String targetField;
    private DbSearcher searcher;

    public Ip2region(String id, Configuration config, Context context) {
        // constructors should validate configuration options
        this.id = id;
        this.sourceField = config.get(SOURCE_CONFIG);
        this.targetField = config.get(TARGET_CONFIG);
        try {
            this.searcher = new DbSearcher(new DbConfig(), config.get(DATABASE));
        } catch ( Exception e ) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    public Collection<Event> filter(Collection<Event> events, FilterMatchListener matchListener) {
        for (Event e : events) {
            Object f = e.getField(sourceField);
            if (f instanceof String) {
                String regioncode;
                try {
                    DataBlock block = searcher.memorySearch((String)f);
                    String[] split = block.getRegion().split("\\|");
                    regioncode = split[1];
                } catch ( Exception error ) {
                    regioncode = "0";
                }
                e.setField(targetField, regioncode);
                matchListener.filterMatched(e);
            }
        }
        return events;
    }

    @Override
    public Collection<PluginConfigSpec<?>> configSchema() {
        // should return a list of all configuration options for this plugin
        List<PluginConfigSpec<?>> list = new ArrayList<>();
        list.add(SOURCE_CONFIG);
        list.add(TARGET_CONFIG);
        list.add(DATABASE);
        return Collections.synchronizedList(list);
    }

    @Override
    public String getId() {
        return this.id;
    }
}
