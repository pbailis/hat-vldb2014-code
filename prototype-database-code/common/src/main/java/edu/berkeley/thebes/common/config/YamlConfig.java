package edu.berkeley.thebes.common.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

import javax.naming.ConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

public class YamlConfig {
    private static Map<String, Object> config;

    private static Logger logger = LoggerFactory.getLogger(YamlConfig.class);

    protected static void initialize(String configFile) throws FileNotFoundException, ConfigurationException {
        config = (Map<String, Object>) (new Yaml()).load(new FileInputStream(new File(configFile)));
        if (config == null)
            config = new HashMap<String, Object>();
    }

    protected static Object getOption(String optionName) {
        return config.get(optionName);
    }
}