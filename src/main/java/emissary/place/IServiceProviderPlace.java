package emissary.place;

import emissary.config.ConfigUtil;
import emissary.config.Configurator;
import emissary.core.IBaseDataObject;
import emissary.core.MobileAgent;
import emissary.core.NamespaceException;
import emissary.core.ResourceException;
import emissary.directory.DirectoryEntry;

import jakarta.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * IServiceProviderPlace. IServiceProviderPlaces can be created by the emissary.admin.PlaceStarter and registered with
 * the emissary.directory.IDirectoryPlace to make their respective services available and a specified cost and quality
 * throughout the system.
 */
public interface IServiceProviderPlace {

    String DOT = ".";

    /**
     * Used as a marker on the transofrm history of a payload when we sprout it, between the parent's history and the new
     * history of the sprout
     */
    String SPROUT_KEY = "<SPROUT>";

    /**
     * Return list of next places to go with data. Delegation call through to our IDirectoryPlace
     * 
     * @param dataId the SERVICE_NAME::SERVICE_TYPE
     * @param lastPlace last place visited
     * @return list of DirectoryEntry
     */
    List<DirectoryEntry> nextKeys(String dataId, IBaseDataObject payload, DirectoryEntry lastPlace);

    /**
     * Add a service proxy to a running place. Duplicates are ignored.
     * 
     * @param serviceProxy the new proxy string to add
     */
    void addServiceProxy(String serviceProxy);

    /**
     * Add a full key to the running place.
     * 
     * @param key the new key
     */
    void addKey(String key);

    /**
     * Remove key
     * 
     * @param key the key to remove
     */
    void removeKey(String key);

    /**
     * Remove a service proxy from the running place. Proxy strings not found registered will be ignored
     * 
     * @param serviceProxy the proxy string to remove
     */
    void removeServiceProxy(String serviceProxy);

    /**
     * Shutdown the place, freeing any resources
     */
    void shutDown();

    /**
     * Visiting agents call here to have a payload processed
     * 
     * @param payload the payload to be processed
     */
    void agentProcessCall(IBaseDataObject payload) throws ResourceException;

    /**
     * Return an entry representing this place in the directory
     */
    DirectoryEntry getDirectoryEntry();

    /**
     * Method called by the HD Agents to process a payload
     * 
     * @param payload the payload to be processed
     * @return list of IBaseDataObject result attachments
     */
    List<IBaseDataObject> agentProcessHeavyDuty(IBaseDataObject payload) throws Exception;

    /**
     * Method called by the HD Agents for bulk processing of payloads
     * 
     * @param payloadList list of payloads to be processed
     * @return list of IBaseDataObject result attachments
     */
    List<IBaseDataObject> agentProcessHeavyDuty(List<IBaseDataObject> payloadList) throws Exception;

    /**
     * Override point for HD Agent calls
     * 
     * @param payload the payload to be processed
     * @return list of IBaseDataObject result attachments
     */
    List<IBaseDataObject> processHeavyDuty(IBaseDataObject payload) throws ResourceException;

    /**
     * Override point for non-HD agent calls
     * 
     * @param payload the payload to be processed
     */
    void process(IBaseDataObject payload) throws ResourceException;

    /**
     * Get key for place, first one on list with '*' as service proxy if there are multiple entries
     * 
     * @return the key of this place in the directory
     */
    String getKey();

    /**
     * List the keys for this place registration
     * 
     * @return list of string complete keys
     */
    Set<String> getKeys();

    /**
     * Return a set of the service proxies
     * 
     * @return set of string values
     */
    Set<String> getProxies();

    /**
     * Return the first service proxy on the list
     * 
     * @return SERVICE_PROXY value or empty string if none
     */
    String getPrimaryProxy();

    /**
     * Get place name
     * 
     * @return string name of the place
     */
    String getPlaceName();

    /**
     * Get custom resource limitation in millis if specified
     * 
     * @return -2 if not specified, or long millis if specified
     */
    long getResourceLimitMillis();


    /**
     * Get the agent that is currently responsible for this thread
     */
    MobileAgent getAgent() throws NamespaceException;

    /**
     * Returns whether form is denied
     */
    boolean isDenied(String s);

    void setPlaceLocation(String placeLocation);

    void setConfigLocations(List<String> configLocations);


    /**
     * Load the configurator
     *
     * @param configStream the stream to use or null to auto configure
     */
    default Configurator loadConfigurator(@Nullable InputStream configStream, String placeLocation) throws IOException {
        // Read the configuration stream
        if (configStream != null) {
            // Use supplied stream
            return ConfigUtil.getConfigInfo(configStream);
        }
        return loadConfigurator(placeLocation);
    }

    /**
     * Load the configurator
     *
     * @param configFileName the file name to use or null to auto configure
     */
    default Configurator loadConfigurator(@Nullable String configFileName, String placeLocation) throws IOException {
        // Read the configuration stream
        if (configFileName != null) {
            // Use supplied stream
            return ConfigUtil.getConfigInfo(configFileName);
        }
        return loadConfigurator(placeLocation);
    }

    /**
     * Load the configurator, figuring out whence automatically
     */
    default Configurator loadConfigurator(@Nullable String placeLocation) throws IOException {
        if (placeLocation == null) {
            placeLocation = this.getClass().getSimpleName();
        }
        setPlaceLocation(placeLocation);

        // Extract config data stream name from place location
        // and try finding config info with and without the
        // package name of this class (in that order)
        String myPackage = this.getClass().getPackage().getName();
        List<String> configLocs = new ArrayList<>();
        // Dont use KeyManipulator for this, only works when hostname/fqdn has dots
        int pos = placeLocation.lastIndexOf("/");
        String serviceClass = pos > -1 ? placeLocation.substring(pos + 1) : placeLocation;
        configLocs.add(myPackage + DOT + serviceClass + ConfigUtil.CONFIG_FILE_ENDING);
        configLocs.add(serviceClass + ConfigUtil.CONFIG_FILE_ENDING);
        setConfigLocations(configLocs);
        return ConfigUtil.getConfigInfo(configLocs);
    }

    /**
     * Reload the {@link Configurator}
     *
     * @param configLocations the list of configuration files to load
     * @throws IOException if there is an issue loading the config
     */
    default Configurator loadConfigurator(@Nullable final List<String> configLocations, final String placeLocation) throws IOException {
        if (CollectionUtils.isNotEmpty(configLocations)) {
            return ConfigUtil.getConfigInfo(configLocations);
        }
        return loadConfigurator(placeLocation);
    }

}
