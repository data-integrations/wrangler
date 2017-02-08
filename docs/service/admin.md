# Administration and Management Services

* [Creating Workspace](#creating-workspace)
* [Deleting Workspace](#deleting-workspace)
* [Upload File to Workspace](#upload-file-to-workspace)
* [Download File from Workspace](#download-file-from-workspace)

**Creating Workspace**
----
  This REST API call creates a workspace or scratch pad for temporarily storing the data to be wrangled in the backend. 
Workspace is identified by an identifier that can be alpha-numeric with only other allowed character as underscore(_).

* **URL**

  `workspaces/:workspaceid`

* **Method:**
  
  `PUT`
  
*  **URL Params**

  _None_

* **Data Params**

  _Not Applicable_

* **Success Response:**

  * **Code:** 200 </br> **Content:** 
  ```
    { 
      'status' : 200,
      'message' : "Successfully created workspace ':workspaceid'"
    }
  ```
 
* **Error Response:**

  * **Code:** 500 Server Error <br />
    **Content:** 
  ```
    { 
      'status' : 500,
      'message' : "<appropriate error message>"
    }
  ```
  **OR**
  
   * **Code:** 500 Server Error <br />
    **Content:** 
  ```
    Unable to route to service <url>
  ```
  

* **Sample Call:**

  ```
    $.ajax({
      url: "${base-url}/workspaces/${workspace}",
      dataType: "json",
      type : "PUT",
      success : function(r) {
        console.log(r);
      }
    });  
  ```

* **Notes:**

  API call will fail if the backend service is not started or if dataset write fails. 
  
**Deleting Workspace**
----
  This REST API call deletes the workspace or scratch pad. This will also delete any data associated with it. 

* **URL**

  `workspaces/:workspaceid`

* **Method:**
  
  `DELETE`
  
*  **URL Params**

  _None_

* **Data Params**

  _Not Applicable_

* **Success Response:**

  * **Code:** 200 </br> **Content:** 
  ```
    { 
      'status' : 200,
      'message' : "Successfully deleted workspace ':workspaceid'"
    }
  ```
 
* **Error Response:**

  * **Code:** 500 Server Error <br />
    **Content:** 
  ```
    { 
      'status' : 500,
      'message' : "<appropriate error message>"
    }
  ```
  **OR**
  
   * **Code:** 500 Server Error <br />
    **Content:** 
  ```
    Unable to route to service <url>
  ```
  

* **Sample Call:**

  ```
    $.ajax({
      url: "${base-url}/workspaces/${workspace}",
      dataType: "json",
      type : "DELETE",
      success : function(r) {
        console.log(r);
      }
    });  
  ```

* **Notes:**

  API call will fail if the backend service is not started or if dataset write fails.   

**Upload File to Workspace**
----
  This REST API call will upload a file to the workspace. The file is split into lines based on line delimiter (eol).

* **URL**

  `workspaces/:workspaceid/upload`

* **Method:**
  
  `POST`
  
*  **URL Params**

  _None_

* **Data Params**

  _Not Applicable_

* **Success Response:**

  * **Code:** 200 </br> **Content:** 
  ```
    { 
      'status' : 200,
      'message' : "Successfully uploaded data to workspace ':workspaceid' (records 1000)"
    }
  ```
 
* **Error Response:**

  * **Code:** 500 Server Error <br />
    **Content:** 
  ```
    { 
      'status' : 500,
      'message' : "Body not present, please post the file containing the records to be wrangle."
    }
  ```
  **OR**
  
   * **Code:** 500 Server Error <br />
    **Content:** 
  ```
    Unable to route to service <url>
  ```
  
  **OR**
  
  * **Code:** 500 Server Error <br />
    **Content:** 
  ```
    { 
      'status' : 500,
      'message' : "<appropriate error message>"
    }
  ```
 
* **Sample Call:**

  ```
    $.ajax({
        url: "${base-url}/workspaces/${workspace}/upload",
        type: 'POST',
        data: data,
        cache: false,
        contentType: 'application/octet-stream',
        processData: false, // Don't process the files
        contentType: false,
        success: function(r) {
          console.log(r);
        },
        error: function(r) {
          console.log(r);
        });
  ```
  
**Download File from Workspace**
----
  This REST API allows to download data stores in the workspace.

* **URL**

  `workspaces/:workspaceid/download`

* **Method:**
  
  `GET`
  
*  **URL Params**

  _None_

* **Data Params**

  _Not Applicable_

* **Success Response:**

  * **Code:** 200 </br> **Content:** 
  ```
    <data stored in workspace>
  ```
 
* **Error Response:**

  * **Code:** 500 Server Error <br />
    **Content:** 
  ```
    { 
      'status' : 500,
      'message' : "No data exists in the workspace. Please upload the data to this workspace."
    }
  ```
  **OR**
  
   * **Code:** 500 Server Error <br />
    **Content:** 
  ```
    Unable to route to service <url>
  ```
  
  **OR**
  
  * **Code:** 500 Server Error <br />
    **Content:** 
  ```
    { 
      'status' : 500,
      'message' : "<appropriate error message>"
    }
  ```
 
* **Sample Call:**

  ```
    $.ajax({
        url: "${base-url}/workspaces/${workspace}/download",
        type: 'GET',
        success: function(r) {
          console.log(r);
        },
        error: function(r) {
          console.log(r);
        });
  ```  
