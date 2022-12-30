/*
 * Copyright (c) 2022. PengYunNetWork
 *
 * This program is free software: you can use, redistribute, and/or modify it
 * under the terms of the GNU Affero General Public License, version 3 or later ("AGPL"),
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 *  You should have received a copy of the GNU Affero General Public License along with
 *  this program. If not, see <http://www.gnu.org/licenses/>.
 */

package py.utils.gitlab;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;
import py.license.RsaUtils;

/**
 * xx.
 */
public class BranchBlocker {

  private static final Logger logger = LoggerFactory.getLogger(BranchBlocker.class);
  private static final String GITLAB_SERVER = "10.0.1.207";

  /**
   * xx.
   */
  public void doCmd(String token, Command command, List<String> params) throws Exception {
    switch (command) {
      case LOCK:
      case UNLOCK:
        lockOrUnlockMaster(token, command.getGitlabCmdName(), params);
        break;
      case LIST:
        listAllbranches(token, params);
        break;
      default:
        break;
    }
  }

  private void lockOrUnlockMaster(String token, String op, List<String> params) throws Exception {
    String projectUri = String.format("http://%s/api/v3/projects?per_page=100", GITLAB_SERVER);
    List<Project> projects = getProjects(token, projectUri);

    String handledProject = "";
    for (Project project : projects) {
      if (params.size() == 1 && params.get(0).equals("all")) {
        // do nothing
      } else {
        if (!params.contains(String.valueOf(project.getId()))) {
          continue;
        }
      }

      String branchUri = String
          .format("http://%s/api/v3/projects/%d/repository/branches?per_page=100",
              GITLAB_SERVER, project.getId());
      List<Branch> branches = getBranches(token, branchUri);

      for (Branch branch : branches) {
        if (branch.getName().equals("master")) {
          String blockMasterUri = String
              .format("http://%s/api/v3/projects/%d/repository/branches/master/%s",
                  GITLAB_SERVER, project.getId(), op);
          getDataFromGitlab(token, blockMasterUri, HttpMethod.PUT);
          branch.setStatus(op);
        }
      }

      handledProject += "\n\t";
      handledProject += project.getName();
    }

    System.out.println("Succeed to " + op + " master branch of projects:" + handledProject + "\n");
  }

  private void listAllbranches(String token, List<String> params) throws Exception {
    String projectUri = String.format("http://%s/api/v3/projects?per_page=100", GITLAB_SERVER);
    List<Project> projects = getProjects(token, projectUri);

    String branchStatusOut = String.format("\t%-10s%-40s%10s\n", "id", "name", "status");
    branchStatusOut += "\t------------------------------------------------------------\n";
    for (Project project : projects) {
      if (params.size() == 1 && params.get(0).equals("all")) {
        // do nothing
      } else {
        if (!params.contains(String.valueOf(project.getId()))) {
          continue;
        }
      }

      String branchUri = String
          .format("http://%s/api/v3/projects/%d/repository/branches?per_page=100",
              GITLAB_SERVER, project.getId());
      List<Branch> branches = getBranches(token, branchUri);

      for (Branch branch : branches) {
        if (branch.getName().equals("master")) {

          String temp = String.format("\t%-10d%-40s%10s\n", project.getId(), project.getName(),
              branch.getStatus().equals("true") ? "locked" : "unlocked");
          branchStatusOut += temp;
        }
      }
    }
    System.out
        .println("\nMaster branch status of all projects are as follows: \n\n" + branchStatusOut);
  }

  private List<Project> getProjects(String token, String uri) throws Exception {
    ResponseEntity<String> response = getDataFromGitlab(token, uri, HttpMethod.GET);
    String json = response.getBody();
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    List<Project> gitlabObject = objectMapper.readValue(json, new TypeReference<List<Project>>() {
    });

    return gitlabObject;
  }

  private List<Branch> getBranches(String token, String uri) throws Exception {
    ResponseEntity<String> response = getDataFromGitlab(token, uri, HttpMethod.GET);
    String json = response.getBody();
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    List<Branch> gitlabObject = objectMapper.readValue(json, new TypeReference<List<Branch>>() {
    });

    return gitlabObject;
  }

  private ResponseEntity<String> getDataFromGitlab(String gitlabToken, String uri,
      HttpMethod httpMethod)
      throws Exception {
    RestTemplate restTemplate = new RestTemplate();

    MultiValueMap<String, String> header = new LinkedMultiValueMap<String, String>();
    header.add("PRIVATE-TOKEN", getGitlabToken(gitlabToken));
    HttpEntity<String> entity = new HttpEntity<String>(header);
    ResponseEntity<String> obj = restTemplate.exchange(uri, httpMethod, entity, String.class);

    return obj;
  }

  private String getGitlabToken(String gitlabToken) throws Exception {
    String publicKey = "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDW64otlXUR+cRUevTQHsyx" 
        + "Ywf8WUJBr3PeCiD+sa9Gx30Bwd/UMYz6dzMeSSG/gjJydoFrLyp1qm2rJ45BPDE++" 
        + "dGK19QG5XVW1qgw2Tgl/W/31eEGBFLKdzvF8YbuiG8QmStWVIKFo" 
        + "7B9Mn9Y3PcK2vdrKN1mosVZhjm/n8rBFQIDAQAB";

    // decrypt
    byte[] decryptData = null;
    try {
      byte[] notBase64 = Base64.decodeBase64(gitlabToken.getBytes());
      decryptData = RsaUtils.decryptByPublicKey(notBase64, publicKey);
    } catch (Exception e) {
      logger.debug("Caught an exception", e);
      throw e;
    }
    return new String(decryptData);
  }

  /**
   * xx.
   */
  public static enum Command {
    LIST("list", null),
    
    LOCK("lock", "protect"),
    
    UNLOCK("unlock", "unprotect");

    private final String cmdName;
    private final String gitlabCmdName;

    private Command(String cmdName, String gitlabCmdName) {
      this.cmdName = cmdName;
      this.gitlabCmdName = gitlabCmdName;
    }

    /**
     * xx.
     */
    public static Command get(String cmdName) throws Exception {
      for (Command cmd : values()) {
        if (cmd.getCmdName().equals(cmdName)) {
          return cmd;
        }
      }

      throw new Exception();
    }

    /**
     * xx.
     */
    public static List<String> list() {
      List<String> cmds = new ArrayList<String>();
      for (Command cmd : values()) {
        cmds.add(cmd.getCmdName());
      }
      return cmds;
    }

    public String getCmdName() {
      return cmdName;
    }

    public String getGitlabCmdName() {
      return gitlabCmdName;
    }
  }
}
