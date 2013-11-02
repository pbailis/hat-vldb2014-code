package edu.berkeley.thebes.common.config;

import java.util.List;

import com.google.common.collect.Lists;

/**
 * RequirementLevels define which apps require which parameters to be configured.
 * 
 * Main usage is something like the CliApp calling
 * RequirementLevel.CLIAPP.getRequiredParameters()
 * and ensuring all parameters are existent.
 */
public enum RequirementLevel {
    COMMON,        // Alias: all requirement levels
    CLIAPP,
    CLIENT_COMMON, // Alias: HAT_CLIENT and TWOPL_CLIENT
    SERVER_COMMON, // Alias: HAT_SERVER and TWOPL_SERVER
    HAT_COMMON,    // Alias: HAT_CLIENT and HAT_SERVER
    HAT_CLIENT,
    HAT_SERVER,
    TWOPL_COMMON,  // Alias: TWOPL_CLIENT, TWOPLY_SERVER, TWOPL_TM
    TWOPL_CLIENT,
    TWOPL_SERVER,
    TWOPL_TM;
    
    private List<ConfigParameters> requiredParams;
    
    /** Get all ConfigParamaters at this requirement level. */
    public List<ConfigParameters> getRequiredParameters() {
        // We cache the result of this computation.
        if (requiredParams != null) {
            return requiredParams;
        }
        
        List<ConfigParameters> params = Lists.newArrayList();
        List<RequirementLevel> parentLevels = getParentLevels();
        for (ConfigParameters param : ConfigParameters.values()) {
            for (RequirementLevel reqLevel : parentLevels) {
                if (param.requiresLevel(reqLevel)) {
                    params.add(param);
                    break;
                }
            }
        }
        
        requiredParams = params;
        return params;
    }
    
    /**
     * This is the inverse of getAliasedLevels -- it returns all RequirementLevels
     * which alias to us (including ourself).
     */
    private List<RequirementLevel> getParentLevels() {
        List<RequirementLevel> parentLevels = Lists.newArrayList();
        for (RequirementLevel reqLevel : RequirementLevel.values()) {
            RequirementLevel[] aliasedLevels = reqLevel.getAliasedLevels();
            for (RequirementLevel aliasedLevel : aliasedLevels) {
                if (aliasedLevel == this) {
                    parentLevels.add(this);
                    break;
                }
            }
        }
        return parentLevels;
    }
    
    /** Expands aliases (e.g., COMMON aliases to everything). */
    private RequirementLevel[] getAliasedLevels() {
        switch (this) {
        case COMMON:
            return RequirementLevel.values();
        case CLIENT_COMMON:
            return new RequirementLevel[] { CLIENT_COMMON, HAT_CLIENT, TWOPL_CLIENT };
        case SERVER_COMMON:
            return new RequirementLevel[] { SERVER_COMMON, HAT_SERVER, TWOPL_SERVER };
        case HAT_COMMON:
            return new RequirementLevel[]{ HAT_COMMON, HAT_SERVER, HAT_CLIENT };
        case TWOPL_COMMON:
            return new RequirementLevel[]{ TWOPL_COMMON, TWOPL_SERVER, TWOPL_CLIENT, TWOPL_TM };
        default:
            return new RequirementLevel[]{ this };
        }
    }
}
