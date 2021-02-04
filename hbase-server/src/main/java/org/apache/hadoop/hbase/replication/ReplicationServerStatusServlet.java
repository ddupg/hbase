/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.replication;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.tmpl.regionserver.RSStatusTmpl;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class ReplicationServerStatusServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
  throws ServletException, IOException {
    HReplicationServer hrs =
      (HReplicationServer) getServletContext().getAttribute(HReplicationServer.REPLICATION_SERVER);
    assert hrs != null : "No ReplicationServer in context!";

    resp.setContentType("text/html");

    if (!hrs.isOnline()) {
      resp.getWriter().write("The ReplicationServer is initializing!");
      resp.getWriter().close();
      return;
    }

    RSStatusTmpl tmpl = new RSStatusTmpl();
    if (req.getParameter("format") != null)
      tmpl.setFormat(req.getParameter("format"));
    if (req.getParameter("filter") != null)
      tmpl.setFilter(req.getParameter("filter"));
    tmpl.render(resp.getWriter(), hrs);
  }
}
