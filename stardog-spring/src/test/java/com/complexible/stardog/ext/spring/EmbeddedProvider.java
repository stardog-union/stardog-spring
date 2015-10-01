package com.complexible.stardog.ext.spring;

import com.complexible.common.protocols.server.ServerException;
import com.complexible.stardog.Stardog;
import com.complexible.stardog.StardogException;
import com.complexible.stardog.api.admin.AdminConnection;
import com.complexible.stardog.api.admin.AdminConnectionConfiguration;
import com.complexible.stardog.protocols.snarl.SNARLProtocolConstants;

/**
 * Created by albaker on 3/4/14.
 */
public class EmbeddedProvider implements Provider {

    @Override
    public void execute(String to, String url, String user, String pass) {

        try {
            Stardog.builder()
		    .create()
		    .newServer()
                    .bind(SNARLProtocolConstants.EMBEDDED_ADDRESS)
                    .start();
            AdminConnection dbms = AdminConnectionConfiguration.toEmbeddedServer().credentials(user, pass).connect();
            if (dbms.list().contains(to)) {
                dbms.drop(to);
                dbms.createMemory(to);
            } else {
                dbms.createMemory(to);
            }
            dbms.close();
        } catch (StardogException e) {


        } catch (ServerException e) {

        } finally {

        }

    }
}
