package org.dbiir.harp.bootstrap;

import org.dbiir.harp.backend.config.ProxyConfigurationLoader;
import org.dbiir.harp.backend.config.YamlProxyConfiguration;
import org.dbiir.harp.bootstrap.arguments.BootstrapArguments;
import org.dbiir.harp.bootstrap.initializer.BootstrapInitializer;
import org.dbiir.harp.frontend.HarpProxy;
import org.dbiir.harp.utils.common.config.props.ConfigurationProperties;
import org.dbiir.harp.utils.common.config.props.ConfigurationPropertyKey;
import org.dbiir.harp.utils.transcation.AgentAsyncXAManager;
import org.dbiir.harp.utils.transcation.AsyncMessageFromAgent;

import java.io.IOException;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

public class Bootstrap {
    /*
     * @params args: args[0] -- port; args[1] -- address/socket_path; args[3] -- config path;
     */
    public static void main(String[] args) throws IOException, SQLException, InterruptedException {
        System.out.println(System.getProperty("java.class.path"));
        BootstrapArguments bootstrapArgs = new BootstrapArguments(preProcessingArgs(args));
        YamlProxyConfiguration yamlConfig = ProxyConfigurationLoader.load(bootstrapArgs.getConfigurationPath());
        ConfigurationProperties configurationProperties = new ConfigurationProperties(yamlConfig.getServerConfiguration().getProps());
        int port = bootstrapArgs.getPort().orElseGet(() -> configurationProperties.getValue(ConfigurationPropertyKey.PROXY_DEFAULT_PORT));
        List<String> addresses = bootstrapArgs.getAddresses();
        new BootstrapInitializer().init(yamlConfig, port, bootstrapArgs.getForce());
        HarpProxy proxy = new HarpProxy();
        // TODO: create sockets for async preparation
//        bootstrapArgs.getSocketPath().ifPresent(proxy::start);
        proxy.start(port, addresses);
    }

    private static String[] preProcessingArgs(final String[] args) {
        List<String> result = new LinkedList<>();
        for (String each : args) {
            if (each.contains("--alg")) {
                String[] split = each.split("=");
                AgentAsyncXAManager.getInstance().setAlgorithm(split[split.length - 1]);
            } else {
                result.add(each);
            }
        }
        return result.toArray(new String[0]);
    }
}