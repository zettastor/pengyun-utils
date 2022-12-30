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

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import py.license.RsaUtils;

/**
 * xx.
 */
@PropertySource("classpath:config/deploy.properties")
@Configuration
public class GitlabConfiguration {

  private static final Logger logger = LoggerFactory.getLogger(GitlabConfiguration.class);

  @Value("${gitlab.authorize.token}")
  protected String gitlabToken;

  /**
   * xx.
   */
  @Bean
  public String getGitlabToken() throws Exception {
    String publicKey = "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCau4gvkI344/Wntvb4IRYPrlukZ" 
        + "/GMDkGn6gXgdWk/xlUDo/rFgypJeeK2qljNZCMUBaHFwIvnXQJSpwpJlXaE9JP5TI+" 
        + "1fLdFTDvJMhO46sQv14xF262UVPBuMnWcxtBD7F3JP+HnF8SD5FGFKEaAepGSIA8R08PVzyarfiQuOQIDAQAB";

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

}
