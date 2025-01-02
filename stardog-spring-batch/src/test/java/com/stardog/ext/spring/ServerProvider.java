package com.stardog.ext.spring;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.function.Supplier;

import com.complexible.common.protocols.server.ServerException;
import com.complexible.stardog.Stardog;
import com.complexible.stardog.StardogException;
import com.complexible.stardog.api.ConnectionCredentials;
import com.complexible.stardog.api.admin.AdminConnection;
import com.complexible.stardog.api.admin.AdminConnectionConfiguration;

/**
 * Created by albaker on 3/4/14.
 */
public class ServerProvider implements Provider {

    private static Stardog stardog;

    private void startServer(final String endpoint) throws URISyntaxException, ServerException {
        URI uri = new URI(endpoint);
        stardog = Stardog.builder().create();
        stardog.newServer().bind(new InetSocketAddress(uri.getHost(), uri.getPort())).start();
    }

    @Override
    public void execute(String to, String url, String user, String pass) {

        try {
            startServer(url);
            AdminConnection dbms = AdminConnectionConfiguration.toServer(url).credentials(user, pass).connect();
            if (dbms.list().contains(to)) {
                dbms.drop(to);
                dbms.newDatabase(to).create();
            } else {
                dbms.newDatabase(to).create();
            }
            dbms.close();
        } catch (StardogException | URISyntaxException | ServerException e) {
            throw new RuntimeException(e);
        }
        finally {

        }
    }

    @Override
    public void execute(String to, String url, Supplier<ConnectionCredentials> supplier) {

        try {
            startServer(url);
            AdminConnection dbms = AdminConnectionConfiguration.toServer(url).credentialSupplier(supplier).connect();
            if (dbms.list().contains(to)) {
                dbms.drop(to);
                dbms.newDatabase(to).create();
            } else {
                dbms.newDatabase(to).create();
            }
            dbms.close();
        } catch (StardogException | URISyntaxException | ServerException e) {
            throw new RuntimeException(e);
        } finally {

        }
    }
}
