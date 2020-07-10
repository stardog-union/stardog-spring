package com.stardog.ext.spring;

import java.util.function.Supplier;

import com.complexible.stardog.Stardog;
import com.complexible.stardog.StardogException;
import com.complexible.stardog.api.ConnectionCredentials;
import com.complexible.stardog.api.admin.AdminConnection;
import com.complexible.stardog.api.admin.AdminConnectionConfiguration;

/**
 * Created by albaker on 3/4/14.
 */
public class EmbeddedProvider implements Provider {

    private static Stardog stardog;

    @Override
    public void execute(String to, String url, String user, String pass) {

        try {
            stardog = Stardog.builder().create();
            AdminConnection dbms = AdminConnectionConfiguration.toEmbeddedServer().credentials(user, pass).connect();
            if (dbms.list().contains(to)) {
                dbms.drop(to);
                dbms.newDatabase(to).create();
            } else {
                dbms.newDatabase(to).create();
            }
            dbms.close();
        } catch (StardogException e) {

        } finally {

        }
    }

    @Override
    public void execute(String to, String url, Supplier<ConnectionCredentials> supplier) {

        try {
            stardog = Stardog.builder().create();
            AdminConnection dbms = AdminConnectionConfiguration.toEmbeddedServer().credentialSupplier(supplier).connect();
            if (dbms.list().contains(to)) {
                dbms.drop(to);
                dbms.newDatabase(to).create();
            } else {
                dbms.newDatabase(to).create();
            }
            dbms.close();
        } catch (StardogException e) {

        } finally {

        }
    }
}
