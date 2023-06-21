package io.cdap.plugin.wrangler.stepsdesign;
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

import io.cdap.e2e.pages.locators.CdfConnectionLocators;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.wrangler.actions.ValidationHelper;
import io.cdap.plugin.wrangler.actions.WranglerPropertiesPageActions;
import io.cdap.plugin.wrangler.locators.WranglerPropertiesPage;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.junit.Assert;
import org.openqa.selenium.WebElement;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Step Design to execute Wrangler plugin test cases.
 */

public class Wrangler implements CdfHelper {

    static {
        SeleniumHelper.getPropertiesLocators(WranglerPropertiesPage.class);
    }

    @Then("Rename the pipeline")
    public void renameThePipeline() {
        WranglerPropertiesPageActions.renameThePipeline();
    }

    @Then("Validate The Data From BQ To BQ With Actual And Expected File for: {string}")
    public void validateTheDataFromBQToBQWithActualAndExpectedFileFor(String expectedFile) throws IOException,
      InterruptedException, URISyntaxException {
        boolean recordsMatched = ValidationHelper.validateActualDataToExpectedData(
                PluginPropertyUtils.pluginProp("bqTargetTable"),
                PluginPropertyUtils.pluginProp(expectedFile));
        Assert.assertTrue("Value of records in actual and expected file is equal", recordsMatched);
    }
}
