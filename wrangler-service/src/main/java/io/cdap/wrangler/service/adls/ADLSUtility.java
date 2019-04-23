/*
 * Copyright © 2018 Cask Data, Inc.
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
package io.cdap.wrangler.service.adls;

import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.DirectoryEntry;
import com.microsoft.azure.datalake.store.DirectoryEntryType;
import io.cdap.wrangler.proto.adls.ADLSDirectoryEntryInfo;
import io.cdap.wrangler.proto.adls.FileQueryDetails;
import io.cdap.wrangler.service.FileTypeDetector;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class ADLSUtility {

    private static final FileTypeDetector detector = new FileTypeDetector();

    public static String testConnection(ADLStoreClient client) throws IOException {
        try {
            client.enumerateDirectory("/");
        } catch (IOException e) {
            throw new IOException("Connection Failed, please check given credentials : " + e.getMessage());
        }
        return "Success";
    }

    public static List<ADLSDirectoryEntryInfo> initClientReturnResponse(ADLStoreClient client, String path)
            throws IOException {
        if (!client.checkExists(path)) {
            throw new IOException("Given path doesn't exist");
        }
        List<DirectoryEntry> list = client.enumerateDirectory(path);
        ADLSDirectoryEntryInfo info;
        List<ADLSDirectoryEntryInfo> directoryEntryInfos = new ArrayList<>();
        for (DirectoryEntry entry : list) {

            if (entry.type.equals(DirectoryEntryType.DIRECTORY)) {
                info = ADLSDirectoryEntryInfo.builder(entry.name, entry.type.toString()).setPath(entry.fullName).
                        setDisplaySize(entry.length).setGroup(entry.group).setUser(entry.user).setPermission
                        (entry.permission)
                        .setLastModified(entry.lastModifiedTime.toString()).setIsDirectory(true).build();
            } else {
                String type = detector.detectFileType(entry.name);
                boolean canWrangle = detector.isWrangleable(type);
                info = ADLSDirectoryEntryInfo.builder(entry.name, entry.type.toString()).setPath(entry.fullName).
                        setDisplaySize(entry.length).setGroup(entry.group).setUser(entry.user).setPermission
                        (entry.permission)
                        .setLastModified(entry.lastModifiedTime.toString()).setIsDirectory(false).setCanWrangle
                                (canWrangle).build();
            }
            directoryEntryInfos.add(info);
        }
        return directoryEntryInfos;
    }

    public static InputStream clientInputStream(ADLStoreClient client, FileQueryDetails fileQueryDetails)
            throws IOException {
        DirectoryEntry file = client.getDirectoryEntry(fileQueryDetails.getFilePath());
        InputStream inputStream = client.getReadStream(file.fullName);
        return inputStream;
    }

    public static DirectoryEntry getFileFromClient(ADLStoreClient client, String path) throws IOException {
        DirectoryEntry file = client.getDirectoryEntry(path);
        return file;
    }

}
