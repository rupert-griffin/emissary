package emissary.grpc.channel;

import emissary.config.Configurator;
import emissary.grpc.channel.ChannelManager.ChannelValidator;

public class ChannelManagerFactory {
    public static final String MANAGER_CLASS_NAME = ChannelManager.GRPC_CHANNEL_PREFIX + "MANAGER_CLASS_NAME";
    public static final String DEFAULT_MANAGER_CLASS_NAME = SharedChannelManager.class.getName();

    private final Configurator configG;
    private final ChannelValidator validator;
    private final String className;

    public ChannelManagerFactory(Configurator configG, ChannelValidator validator) {
        this.configG = configG;
        this.validator = validator;
        this.className = configG.findStringEntry(MANAGER_CLASS_NAME, DEFAULT_MANAGER_CLASS_NAME);
    }

    public ChannelManager build(String host, int port) {
        try {
            return Class.forName(className)
                    .asSubclass(ChannelManager.class)
                    .getDeclaredConstructor(String.class, int.class, Configurator.class, ChannelValidator.class)
                    .newInstance(host, port, configG, validator);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Unable to instantiate " + className, e);
        }
    }
}
