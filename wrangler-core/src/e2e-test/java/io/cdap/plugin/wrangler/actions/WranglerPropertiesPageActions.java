/*
 * Copyright © 2023 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.wrangler.actions;
import io.cdap.e2e.pages.actions.CdfStudioActions;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.e2e.utils.WaitHelper;
import io.cdap.plugin.wrangler.locators.WranglerPropertiesPage;
import org.apache.commons.lang3.RandomStringUtils;
import org.openqa.selenium.NoSuchElementException;
import org.openqa.selenium.WebElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrangler - Properties page - Actions.
 */
public class WranglerPropertiesPageActions {

    private static final Logger logger = LoggerFactory.getLogger(ElementHelper.class);

    static {
        SeleniumHelper.getPropertiesLocators(WranglerPropertiesPage.class);
    }
    public static String pipelineName;

    public static void renameThePipeline() {
        WaitHelper.waitForElementToBeOptionallyDisplayed(WranglerPropertiesPage.renamePipeline(), 100);
        ElementHelper.clickOnElement(WranglerPropertiesPage.appendPipeline);
        pipelineName = "TestPipeline-" + RandomStringUtils.randomAlphanumeric(10);
        WranglerPropertiesPageActions.fillPipelineNameAndSave(pipelineName);
    }

    public static void fillPipelineNameAndSave(String pipelineName) {
        pipelineNameIp(pipelineName);
        pipelineSave();
        WaitHelper.waitForElementToBeDisplayed(WranglerPropertiesPage.statusBanner);
        WaitHelper.waitForElementToBeHidden(WranglerPropertiesPage.statusBanner);
    }

    public static void pipelineNameIp(String pipelinName) {
        ElementHelper.sendKeys(WranglerPropertiesPage.pipelineNameIp, pipelinName);
    }

    public static void pipelineSave() {
        ElementHelper.clickOnElement(WranglerPropertiesPage.pipelineSave);
    }
}
