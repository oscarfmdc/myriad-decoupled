/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package webapp;

import javax.inject.Inject;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.DefaultServlet;
import org.mortbay.jetty.servlet.FilterMapping;
import org.mortbay.jetty.servlet.ServletHandler;

import java.util.Objects;

/**
 * The myriad web server configuration for jetty
 */
public class MyriadWebServer {
    private final Server jetty;
    private final Connector connector;

    /**
     * Status codes for MyriadWebServer
     */
    public enum Status {STARTED, RUNNING, STOPPED, FAILED, UNKNOWN}

    @Inject
    public MyriadWebServer(Server jetty, Connector connector) {
        this.jetty = jetty;
        this.connector = connector;
    }

    public void start() throws Exception {
        this.jetty.addConnector(connector);

        ServletHandler servletHandler = new ServletHandler();

        FilterMapping filterMapping = new FilterMapping();
        filterMapping.setPathSpec("/*");
        filterMapping.setDispatches(Handler.ALL);

        Context context = new Context();
        context.setServletHandler(servletHandler);
        context.addServlet(DefaultServlet.class, "/");

        String staticDir = Objects.requireNonNull(this.getClass().getClassLoader().getResource("webapp/public")).toExternalForm();
        context.setResourceBase(staticDir);

        this.jetty.addHandler(context);
        this.jetty.start();
    }

    public Status getStatus() {
        if (jetty.isFailed()) {
            return Status.FAILED;
        } else if (jetty.isStarted()) {
            return Status.STARTED;
        } else if (jetty.isRunning()) {
            return Status.RUNNING;
        } else if (jetty.isStopped()) {
            return Status.STOPPED;
        } else {
            return Status.UNKNOWN;
        }
    }

    public void stop() throws Exception {
        this.jetty.stop();
        this.connector.close();
    }
}