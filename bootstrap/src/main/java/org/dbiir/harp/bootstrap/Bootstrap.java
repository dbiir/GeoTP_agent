package org.dbiir.harp.bootstrap;

import org.dbiir.harp.backend.config.ProxyConfigurationLoader;
import org.dbiir.harp.backend.config.YamlProxyConfiguration;
import org.dbiir.harp.bootstrap.arguments.BootstrapArguments;
import org.dbiir.harp.bootstrap.initializer.BootstrapInitializer;
import org.dbiir.harp.frontend.HarpProxy;
import org.dbiir.harp.utils.common.config.props.ConfigurationProperties;
import org.dbiir.harp.utils.common.config.props.ConfigurationPropertyKey;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

public class Bootstrap {
    /*
     * @params args: args[0] -- port; args[1] -- address/socket_path; args[3] -- config path;
     */
    public static void main(String[] args) throws IOException, SQLException, InterruptedException {
        System.out.println(System.getProperty("java.class.path"));
        BootstrapArguments bootstrapArgs = new BootstrapArguments(args);
        YamlProxyConfiguration yamlConfig = ProxyConfigurationLoader.load(bootstrapArgs.getConfigurationPath());
        ConfigurationProperties configurationProperties = new ConfigurationProperties(yamlConfig.getServerConfiguration().getProps());
        int port = bootstrapArgs.getPort().orElseGet(() -> configurationProperties.getValue(ConfigurationPropertyKey.PROXY_DEFAULT_PORT));
        List<String> addresses = bootstrapArgs.getAddresses();
        new BootstrapInitializer().init(yamlConfig, port, bootstrapArgs.getForce());
        HarpProxy proxy = new HarpProxy();
        // TODO: create sockets for async preparation
        proxy.startAsyncMessageInternal(configurationProperties.getValue(ConfigurationPropertyKey.PROXY_COORDINATOR_DEFAULT_PORT),
                configurationProperties.getValue(ConfigurationPropertyKey.PROXY_COORDINATOR_DEFAULT_IP));
        bootstrapArgs.getSocketPath().ifPresent(proxy::start);
        proxy.start(port, addresses);
    }
}